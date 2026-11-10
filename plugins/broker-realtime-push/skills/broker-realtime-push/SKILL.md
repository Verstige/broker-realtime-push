---
name: broker-realtime-push
description: "Push live broker data to browser tabs for brokers with no streaming API (TradeLocker, DXTrade, MT5 bridges). A server-side poll fans out deltas to every open tab over SSE."
version: 1.0.0
license: MIT
author: "the maintainers"
---
# Broker Realtime Push Pattern

A polling fan-out architecture for brokers that don't natively stream. The
server polls the broker on a slow cadence — once per connected user, not
once per browser tab — and emits SSE deltas to every open tab. Replaces
per-browser polling that would otherwise multiply broker request volume by
the number of tabs your users have open.

## Why this exists

Real brokers that have no usable streaming endpoint for account state:

- **the broker** (GenFX): REST only — no WebSocket, no Server-Sent Events,
  no long-poll. Every state read is a HTTP roundtrip.
- **MT5 / MetaTrader bridges** (MetaApi, custom expert advisors): HTTP
  polling required.
- **DXTrade**: REST only.
- **MatchTrader**: REST only.
- **IB** (Interactive Brokers): account state arrives through a session that
  must be polled for a read-many snapshot.
- **cTrader**: the Open API does have streaming channels, but most
  integrations only need account/position state and read it through the
  REST endpoints — the same fan-out pattern applies to that path.
- **IB** (TWS / gateway APIs): account state arrives through a session
  that must be polled for a read-many snapshot.

Naive architecture: every browser tab polls `/api/account-data` every
5–15s. With 50 users × 4 tabs each, that's 200 requests/minute hitting the
broker. Cloudflare 1015s the entire app within minutes. This is a common
failure mode and it is expensive to diagnose, because the dashboard looks
perfect locally and only breaks once real traffic arrives.

The fix: **one polling loop per server process, per connected user.** The
browser opens an SSE connection to the server. The server polls the broker
on the user's behalf at a slow cadence and pushes deltas down the SSE pipe.

```
Browser tab (Overview dashboard)      Browser tab (Trading terminal)
        │                                     │
        │     SSE: open /api/stream           │   SSE: open
        │            │                          │
        │            ▼                          ▼
        │      ┌───────────────────────────────┐
        │      │   In-process broker            │
        │      │   - subscribersByUser Map     │
        │      │   - per-user poller state     │
        │      │   - 30s cadence               │
        │      └───────────────────────────────┘
        │                     │
        │                     ▼
        │              Broker REST API
        │              (1 request / 30s / user)
        │
        ▼
  applied to local state
```

## The pattern (worked)

### Three files

1. **`lib/realtime/broker.ts`** — module-scoped state and the
   subscribe/poll API.
2. **`app/api/stream/route.ts`** — the SSE endpoint the browser connects to.
3. **`app/api/poll/route.ts`** — server-to-server broker poll. Authenticated
   with a shared secret header so it can't be called from outside the
   process.

(Paths are shown for the Next.js App Router layout; translate to your
framework's route conventions.)

### Module-scoped broker state

```ts
type Subscriber = { userId: string; send: (data: string) => void }
type UserState = {
    snapshot: { positions, balance, equity, margin, freeMargin } | null
    timer: NodeJS.Timeout | null
    inFlight: boolean
    lastErrorAt: number
}

const subscribersByUser = new Map<string, Set<Subscriber>>()
const userState = new Map<string, UserState>()
```

Module-scoped state is fine for production: a single Node process holds it
for its lifetime. HMR in dev mode will reset it but the polling loop and
subscribers re-establish on the next request.

### Subscribe / unsubscribe lifecycle

```ts
export function subscribe(userId: string, send: (data: string) => void): () => void {
    let subs = subscribersByUser.get(userId)
    if (!subs) { subs = new Set(); subscribersByUser.set(userId, subs) }
    subs.add({ userId, send })
    startUserPoller(userId)              // ref-counted: only starts if needed
    return () => {
        subs?.delete(...)
        if (subs?.size === 0) {
            subscribersByUser.delete(userId)
            stopUserPoller(userId)       // last unsub → stop polling
        }
    }
}
```

The reference counting is the magic. **First subscriber for user X → start
30s polling loop for X. Last unsubscriber → stop the loop.** Zero polling
while nobody's connected.

### Polling cadence

- **30s** per user. the broker and most broker REST APIs have undocumented
  per-endpoint throttling; 30s with an N-symbol cache is comfortable.
- **25s keepalive pings** on the SSE pipe. Most SSE proxies (Cloudflare,
  Nginx) close idle connections without traffic.
- **60s backoff** on poll error. If the broker returned 429 or 5xx, wait a
  full minute before retrying.

### Event taxonomy

Emit **deltas**, not full state. The browser already has the data — it just
needs to know what changed.

```ts
// On first poll (no snapshot yet):
emit(userId, 'snapshot', { positions, balance, equity, ... })

// On subsequent polls, compute delta against previous snapshot:
const added = newSnapshot.positions.filter(p => !prevIds.has(p.id))
const removed = prev.positions.filter(p => !newIds.has(p.id))
const modified = newSnapshot.positions.filter(p => {
    const old = prev.positions.find(q => q.id === p.id)
    return old && (old.pnl !== p.pnl || old.avgPrice !== p.avgPrice)
})

if (added.length || removed.length || modified.length || accountChanged) {
    emit(userId, 'delta', { added, removed, modified, account, equity, ... })
}
```

### SSE route shape

```ts
export async function GET(req: NextRequest) {
    const userId = await getUserId(req)
    if (!userId) return new Response('Not authenticated', { status: 401 })

    const stream = new ReadableStream({
        start(controller) {
            const send = (data: string) => controller.enqueue(new TextEncoder().encode(data))
            send(': realtime stream open\n\n')              // comment to flush headers

            const unsubscribe = subscribe(userId, send)

            // Keepalive ping (in case the broker's own heartbeat doesn't fire)
            const keepalive = setInterval(() => send(`event: ping\ndata: ${Date.now()}\n\n`), 20_000)

            req.signal.addEventListener('abort', () => {
                clearInterval(keepalive); unsubscribe(); controller.close()
            }, { once: true })
        }
    })

    return new Response(stream, {
        headers: {
            'Content-Type': 'text/event-stream; charset=utf-8',
            'Cache-Control': 'no-cache, no-transform',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',                       // disable Nginx buffering
        },
    })
}
```

`X-Accel-Buffering: no` is critical on Railway and on any Nginx-style
reverse proxy — without it, the proxy buffers SSE chunks and never flushes
them to the client.

### Client-side hook (React)

```tsx
useEffect(() => {
    if (typeof window === 'undefined') return
    if (!brokerConnected) return
    const es = new EventSource('/api/stream', { withCredentials: true })

    const handleSnapshot = (e: MessageEvent) => {
        const snap = JSON.parse(e.data)
        if (Array.isArray(snap?.positions)) setBrokerPositions(snap.positions)
        if (typeof snap?.account === 'number') setAccountBalance(snap.account)
        if (typeof snap?.equity === 'number') setAccountEquity(snap.equity)
    }
    const handleDelta = (e: MessageEvent) => {
        const d = JSON.parse(e.data)
        if (Array.isArray(d?.added) || Array.isArray(d?.removed) || Array.isArray(d?.modified)) {
            setBrokerPositions(prev => {
                const byId = new Map(prev.map(p => [String(p.id), p]))
                for (const r of d.removed ?? []) byId.delete(String(r.id))
                for (const m of [...(d.modified ?? []), ...(d.added ?? [])]) byId.set(String(m.id), m)
                return Array.from(byId.values())
            })
        }
    }

    es.addEventListener('snapshot', handleSnapshot as EventListener)
    es.addEventListener('delta', handleDelta as EventListener)
    es.addEventListener('ping', () => { /* keepalive */ })
    es.addEventListener('error', () => { /* EventSource auto-reconnects */ })

    return () => { es.close() }
}, [brokerConnected])
```

`EventSource` auto-reconnects on error with exponential backoff. The next
`snapshot` event rebuilds local state.

### Server-to-server poll auth

The broker module calls `/api/poll` to do the actual REST work. This route
should NOT be callable from the public internet — only from the
in-process broker. Use a shared secret carried in a header:

```ts
// Server-only config module. The secret is injected by the deployment's
// environment configuration at boot and is never exposed to the browser.
import { BROKER_SHARED_SECRET } from '@/lib/server/secrets'

// In the route handler:
const brokerHeader = req.headers.get('x-broker-secret')
const isBroker = Boolean(brokerHeader) && brokerHeader === BROKER_SHARED_SECRET

let userId: string | null = null
if (isBroker) {
    userId = (await req.json()).userId        // trust the caller
} else {
    userId = await getUserIdOrServiceToken(req) // standard auth
}

// In the broker module:
const res = await fetch(`${serviceBaseUrl}/api/poll`, {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
        'X-Broker-Secret': BROKER_SHARED_SECRET,
    },
    body: JSON.stringify({ userId }),
})
```

Set the shared secret in your deployment's environment configuration
(Railway service variables, container env, or your platform's secret
manager). Anything random works — generate 32 random bytes and hex-encode
them. Rotate it if it ever appears in a log, a screenshot, or a commit.

## Performance characteristics

With this pattern:
- **Per user**: 1 broker request / 30s. 100 users = 100 requests / 30s =
  3.3 req/s.
- **Per browser tab**: zero direct broker calls. All updates via SSE.
- **Per page load**: 1 initial `/api/account-data` (the standard route, no
  SSE). Subsequent updates via SSE.

Compare to naive polling:
- **Per user**: 4 tabs × 4 requests / 15s = 16 req/s per user. 100 users =
  1600 req/s. Cloudflare 1015s in minutes.

The SSE pattern drops broker load by ~500x. That's the whole point.

## Scaling limitations

- **Per-process state.** If the platform runs >1 instance, each instance
  polls its own connected users. Still better than per-browser polling
  (only ~2x broker load vs 100x), but the real fix at scale is to move
  broker state to shared storage — Redis, or PostgreSQL via PostgREST with
  a JSONB column keyed by user (the same persistent-cache shape used by the
  `broker-rate-limit-cache` skill in this category).
- **First request after deploy hits empty broker state.** Until a user
  connects and the broker fetches once, there's no data. The
  reconnect-from-navigation pattern works (SSE re-establishes), but the
  first request still has to hit the broker. Document this for users.
- **JWT expiry mid-session.** If broker tokens expire, the next poll returns
  401. The poll route must refresh the token before calling the broker, and
  retry once on a fresh token. Put the refresh in a single helper so every
  route goes through it.

## Pitfalls

- **Forgetting `X-Accel-Buffering: no`.** Without this, the reverse proxy
  buffers SSE chunks and the browser never sees them. Symptom: the
  connection opens, the `snapshot` event never arrives. Test in production,
  not localhost.
- **Polling too aggressively.** 30s is the right default. 5s client polling
  + 5s server polling = 200+ req/s per user. Don't do it.
- **Sending full state on every poll.** Send deltas. The browser already
  has the data. 100 positions × every poll = wasted bytes.
- **Holding a stale reference in `setInterval`.** If the broker module
  reloads (HMR in dev), the old interval keeps firing against the new
  state. Use a `cleanup` function that clears the timer on each
  `subscribe`.
- **Per-user poller races.** If a poll is in-flight (`inFlight: true`) and
  the interval fires again, skip — don't queue a second fetch. Two
  simultaneous polls to the broker means two chances to get rate-limited.
- **Not unsubscribing on SSE close.** If the browser navigates away
  mid-poll, the subscriber needs to be removed or the poller stays alive
  forever. The `req.signal.addEventListener('abort', cleanup)` pattern
  handles this.
- **Treating SSE as reliable delivery.** EventSource auto-reconnects on
  disconnect, but messages sent during the gap are lost. For trading data
  this is OK (the next snapshot rebuilds state). For user-visible
  notifications, use a different mechanism (Supabase Realtime channels for
  chat or activity feeds).
- **The SSE handler only updates the broker-shape state, but the UI renders
  from a different shape.** This is the most expensive trap to discover
  post-hoc, and it is a common failure mode: the SSE handler in your
  trading context provider updated `setTlPositions` on every `snapshot` and
  `delta` event, but the dashboard's "Live Positions" cards rendered from
  `signals` (a separate state array). Result: SSE updates arrived,
  `tlPositions` was current, the bottom-positions table updated — but the
  overview dashboard cards stayed empty until the next full refresh. Two
  state arrays silently diverged. **Fix:** the SSE handlers MUST mirror
  `tlPositions` into whatever array the dashboard renders from. Pattern:
  ```ts
  // In handleSnapshot AND handleDelta:
  const mapTlToSignal = (pos: any) => ({
      id: pos.id, provider: 'the broker', providerRank: 'Live',
      pair: pos.symbol || pos.tradableInstrumentId, action: pos.side.toUpperCase(),
      price: (pos.avgPrice ?? 0).toString(),
      sl: pos.stopLoss ? String(pos.stopLoss) : '',
      tp1: pos.takeProfit ? String(pos.takeProfit) : '',
      lotSize: pos.qty, profit: pos.pnl, timestamp: pos.openDate || 'Live',
      // ... fields your dashboard reads
  })
  setTlPositions(next)
  setSignals(next.map(mapTlToSignal))   // ← mirror to render-shaped state
  ```
  Whenever you add a new state array that the dashboard renders from, find
  every place that updates the raw broker state and add the mirror. The SSE
  handlers, the auto-connect path, and the manual-refresh path are the
  three most common places to forget this. **Always write a one-line
  integration test that mounts the dashboard, fires a delta, and asserts the
  rendered card count matches the SSE event's `added.length`.** See
  `references/two-state-divergence-trap.md` for the full audit checklist and
  a 30-line integration test that catches every divergence.

## When NOT to use this pattern

- **The broker has native streaming.** If the broker adds a WebSocket or
  SSE endpoint, use that directly. The shared-broker polling pattern is a
  workaround for REST-only brokers.
- **You need sub-second latency.** The 30s polling cadence means updates
  are at most 30s stale. For real-time trade execution, the polling
  pattern is wrong — you need push from the broker, or accept that the
  user's optimistic UI fills the gap.
- **You have one user.** If only one user uses the app, just have the
  client poll directly. The shared-broker architecture is for many users
  sharing one broker connection per user.
- **The data is small and changes rarely.** If the broker only updates once
  an hour, polling every 5 minutes is fine. No need for SSE.

## See also

- `references/two-state-divergence-trap.md` — the audit that found the SSE
  handler, the auto-connect path, and the manual-refresh path all updated
  different state arrays. The dashboard rendered from `signals` but only
  ONE of those three paths populated it. Plus: the API routes didn't return
  an `analytics` block, so the 4 dashboard analytics cards always showed 0.
  The fix is one helper that mirrors to all render-state arrays, called
  from every code path.
- `broker-rate-limit-cache` (sibling skill in this category) — the
  persistent, DB-backed cache for the broker responses this pattern still
  has to fetch. Read it before building the poll loop: the single most
  common bug there (a cache key that includes the requested time window,
  so the cache never hits) also applies to the polled endpoints.

## Worked example: the broker on a Next.js dashboard

A production dashboard shipped this pattern after hitting 100x broker load
and Cloudflare 1015s. Before: every browser tab polled
`/api/account-data` every 15s. After: one polling loop per connected
user, fan-out via SSE, ~3.3 req/s to the broker even at peak load.

- Broker module: `lib/realtime/broker.ts`
- SSE route: `app/api/stream/route.ts`
- Server-to-server poll: `app/api/poll/route.ts`
- Client hook: in the trading context provider, the SSE useEffect opens the
  EventSource and applies `snapshot`/`delta` events to
  `setTlPositions`, `setAccountBalance`, etc.
- Polling cadence: 30s for the broker, 60s safety-net polling on the
  client (was 15s, now serves as fallback if SSE dies).

The 15s→60s polling interval change on the client is the *other* half of
the rate-limit fix. SSE handles updates; polling is just a safety net. With
SSE working, the 60s poll rarely fires.
