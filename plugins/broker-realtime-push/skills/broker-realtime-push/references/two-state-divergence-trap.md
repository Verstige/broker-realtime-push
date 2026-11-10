# Two-state divergence: SSE updates the wrong array

The most expensive bug class in any trading-context + dashboard setup:
**the SSE handler updates one state array, but the dashboard renders from a
different one.** They silently diverge.

## Real symptom

The user reported:
- "Dashboard always shows 0 live positions, even when I have open trades"
- "I have to refresh the page manually to see new positions"
- "Connected status pill is fine, but the cards are blank"

The bug had been shipping for weeks. The overview dashboard's "Live Positions"
cards rendered from `signals` (a `SignalData[]` array mapped from raw broker
positions). The SSE broker in the trading context provider updated
`tlPositions` (a `TLPosition[]` array, raw shape). The two were parallel —
never synced. So:

| Path | Updated `tlPositions`? | Updated `signals`? | UI? |
|---|---|---|---|
| SSE snapshot | yes | no | bottom table refreshes, overview cards blank |
| SSE delta | yes | no | same |
| `doReconnect` (auto-connect) | no | no | only updates balance, equity, connected flag |
| `refreshTradeLockerData` | no | yes | only fires on explicit refresh |

Three out of four code paths updated `tlPositions` but not `signals`. The
fourth updated `signals` but not via the SSE handlers. Result: nothing was
consistent.

This is a common failure mode, and it typically costs several iterations to
diagnose because the build is clean, the network traffic looks right, and
the SSE stream is demonstrably delivering events. The data arrives and the
UI ignores it.

## The audit checklist (use when "dashboard shows wrong data")

1. **Open the trading context provider** and find every place that mutates
   positions: `setTlPositions`, `setSignals`, `setResults`. There should be
   exactly one helper that handles all three, and every state-update path
   should call it.
2. **Open every API route** that returns positions or history:
   `/api/account`, `/api/account-data`, `/api/poll`, `/api/position/[id]`.
   Check that the response shape matches what the dashboard renders from.
3. **Grep for `analytics.total_trades`, `tradeLockerData.analytics.X`** in
   the dashboard component. If the API routes don't return an `analytics`
   block, the dashboard falls back to `results.length` / `signals.length`
   which are usually empty. Dashboard shows 0 for everything.
4. **Open DevTools Network tab.** Filter on the account routes. Click around
   the dashboard. Are the requests firing? Are the responses showing the
   right data? Is anything being CORS-blocked or 401'd?
5. **Check the SSE stream in DevTools.** Network → filter on the stream
   route → click on the EventStream. Are `snapshot`/`delta` events flowing?
   Does the `data:` payload contain the right shape?

If any of these show gaps, that's the bug. Don't ship speculative fixes —
fix the specific gap and ship one commit.

## The fix pattern (apply to all four code paths)

```ts
// 1) Centralize the mapping
const mapTlToSignal = (pos: any) => ({
    id: pos.id,
    provider: 'the broker',
    providerRank: 'Live',
    pair: pos.symbol || pos.tradableInstrumentId,
    action: (pos.side || 'BUY').toUpperCase(),
    price: (pos.avgPrice ?? 0).toString(),
    sl: pos.stopLoss ? String(pos.stopLoss) : '',
    tp1: pos.takeProfit ? String(pos.takeProfit) : '',
    lotSize: pos.qty,
    profit: pos.pnl,
    timestamp: pos.openDate || 'Live',
})

// 2) In SSE handlers
const handleSnapshot = (e) => {
    const snap = JSON.parse(e.data)
    if (Array.isArray(snap?.positions)) {
        setTlPositions(snap.positions)
        setSignals(snap.positions.map(mapTlToSignal))  // ← mirror
    }
    // ... balance/equity
}
const handleDelta = (e) => {
    const d = JSON.parse(e.data)
    if (Array.isArray(d?.added) || Array.isArray(d?.removed) || Array.isArray(d?.modified)) {
        setTlPositions(prev => {
            const byId = new Map(prev.map(p => [String(p.id), p]))
            for (const r of d.removed ?? []) byId.delete(String(r.id))
            for (const m of [...(d.modified ?? []), ...(d.added ?? [])]) byId.set(String(m.id), m)
            const next = Array.from(byId.values())
            setSignals(next.map(mapTlToSignal))  // ← mirror
            return next
        })
    }
}

// 3) In doReconnect (auto-connect path)
if (data.connected) {
    setTradeLockerConnected(true)
    setTradeLockerData(data)
    setAccountBalance(data.balance ?? 0)
    setAccountEquity(data.equity ?? data.balance ?? 0)
    if (Array.isArray(data.positions)) {
        setSignals(data.positions.map(mapTlToSignal))  // ← mirror
    }
    if (Array.isArray(data.recentHistory)) {
        setResults(/* map same shape as refreshTradeLockerData */)
    }
    // ...
}

// 4) In refreshTradeLockerData (explicit refresh)
if (data.positions) {
    setSignals(data.positions.map(mapTlToSignal))  // already correct in current code
}
```

## The "analytics" trap

The 4 dashboard cards (Total Trades, Win Rate, Open Trades, Total P&L) read
from `tradeLockerData.analytics.X`. If the API route doesn't return an
`analytics` block, the cards fall through to `results.length` /
`signals.length` which are also empty. Result: all 4 cards show 0.

Compute analytics server-side from `rawHistory` and return it in every route
that returns positions:

```ts
const totalClosedTrades = rawHistory.length
const winningTrades = rawHistory.filter(t => Number(t.pnl ?? 0) > 0).length
const winRate = totalClosedTrades > 0
    ? Number(((winningTrades / totalClosedTrades) * 100).toFixed(1))
    : 0
const totalPnl = Number(
    rawHistory.reduce((acc, t) => acc + Number(t.pnl ?? 0), 0).toFixed(2)
)
const analytics = {
    total_trades: totalClosedTrades,
    win_rate: winRate,
    total_pnl: totalPnl,
    open_positions: rawPositions.length,
    today_net: todayNet,
    today_gross: openGrossPnL,
}
return NextResponse.json({ ...rest, analytics })
```

Apply this to every broker route that returns positions: `/api/account`,
`/api/account-data`, any future broker route.

## Three-state convergence test (write this once, run forever)

A 30-line integration test that catches every divergence:

```ts
// In a Playwright test or similar:
test('dashboard data populates from all 3 paths', async ({ page }) => {
    await page.goto('/trading')

    // 1) Wait for SSE snapshot
    await page.waitForResponse(r => r.url().includes('/api/stream'))

    // 2) Verify dashboard reads from signals (rendered card count)
    const cardCount = await page.locator('.position-card').count()
    const signalsLength = await page.evaluate(() =>
        // Read the trading context state somehow — instrumented test hook
        window.__tradingContextState?.signals?.length ?? 0
    )
    expect(cardCount).toBe(signalsLength)

    // 3) Verify totalPnl card shows non-zero when positions exist
    const totalPnlText = await page.locator('.analytics-card:has-text("Total P&L") .analytics-number').textContent()
    const totalPnl = parseFloat(totalPnlText.replace(/[+$]/g, ''))
    expect(totalPnl).not.toBe(0)  // or assert specific value
})
```

The `expect(cardCount).toBe(signalsLength)` line catches the two-state
divergence bug instantly. Without it, the bug ships and the user reports it.

## Why "make it work, then test" is wrong here

The natural fix is: "just update signals in the SSE handler." Then the
dashboard still doesn't update because `doReconnect` runs first and
overwrites. Then "just update signals in doReconnect too." Then the analytics
cards are still 0 because the route doesn't return analytics. Then "add
analytics to the route." Then the analytics still don't show because
`setTradeLockerData(data)` doesn't merge in the analytics block.

Each "just add X" is correct in isolation. The bug is that there are 3+ code
paths to maintain, and the dashboard's "render state" array is a 4th
implicit state that none of them know about.

The right fix is: **centralize the state mapping in one helper, then call
that helper from all paths.** One helper. Every path. Mirror to render
state at every update.

## Cost of getting this wrong

This class of bug ships for weeks. The user sees 0 live positions on the
dashboard for the entire stretch. Multiple "fix" commits don't help because
the bug is in the state convergence, not the request flow. **The audit +
3-file fix takes one commit** to land once the right diagnosis is made.

The lesson: **when a "data" feature is broken, audit ALL paths that touch
the data, not just the one the user is currently looking at.** 5 minutes of
audit > 6 commits of speculative fixing.
