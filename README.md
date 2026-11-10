# Realtime Trading Data: SSE Fan-Out for REST Brokers

> Give a REST-only broker a live feed. One server-side poll fans out deltas to every open tab over SSE, instead of each browser tab polling the broker on its own.

![License: MIT](https://img.shields.io/badge/License-MIT-green.svg) ![Claude Code](https://img.shields.io/badge/Claude_Code-skill-blueviolet.svg) [![Available on Agensi](https://img.shields.io/badge/Agensi-%249-ff5a5f.svg)](https://agensi.io/skills/realtime-trading-data-sse-fan-out-for-rest-brokers)

## What it does

Push live broker data to browser tabs for brokers with no streaming API (TradeLocker, DXTrade, MT5 bridges). A server-side poll fans out deltas to every open tab over SSE.

A polling fan-out architecture for brokers that don't natively stream. The
server polls the broker on a slow cadence — once per connected user, not
once per browser tab — and emits SSE deltas to every open tab. Replaces
per-browser polling that would otherwise multiply broker request volume by
the number of tabs your users have open.

## The failure it prevents

The cases that cost time are specific, and they are written down:

- **Forgetting `X-Accel-Buffering: no`.** Without this, the reverse proxy
- **Polling too aggressively.** 30s is the right default. 5s client polling
- **Sending full state on every poll.** Send deltas. The browser already
- **Holding a stale reference in `setInterval`.** If the broker module
- **Per-user poller races.** If a poll is in-flight (`inFlight: true`) and
- **Not unsubscribing on SSE close.** If the browser navigates away

## When to use it

- give a REST-only broker a live feed
- cut broker request volume by not polling per browser tab
- push position and equity deltas to connected clients

## What's inside

```
plugins/
└── broker-realtime-push/
    ├── .claude-plugin/plugin.json
    └── skills/broker-realtime-push/
        ├── SKILL.md
        ├── references/            (1 files)
        │   └── two-state-divergence-trap.md
```

## Install

Add this repo as a Claude Code marketplace, then install the skill:

```bash
claude plugin marketplace add Verstige/broker-realtime-push
claude plugin install broker-realtime-push@broker-realtime-push
```

Or copy the skill straight into your skills directory:

```bash
git clone https://github.com/Verstige/broker-realtime-push.git
cp -R broker-realtime-push/plugins/broker-realtime-push/skills/broker-realtime-push ~/.claude/skills/
```

## Usage

The skill activates on the work it describes. Ask for the outcome, and it
supplies the method:

```
> "give a REST-only broker a live feed"
```

## Reference material

Loaded on demand — the skill only reads what the task needs.

| File | Covers |
| --- | --- |
| `two-state-divergence-trap.md` | Two-state divergence: SSE updates the wrong array |

## License

MIT. See [LICENSE](LICENSE).

---

**Also available as a paid skill on Agensi** — $9. Install it with one command, no clone required:

[agensi.io/skills/realtime-trading-data-sse-fan-out-for-rest-brokers](https://agensi.io/skills/realtime-trading-data-sse-fan-out-for-rest-brokers)

Part of the [Verstige](https://github.com/Verstige) skills portfolio. More at [github.com/Verstige](https://github.com/Verstige).
