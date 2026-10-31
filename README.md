# Binance Live Market Data — MCP Server

A **read-only Model Context Protocol (MCP) server** that gives AI assistants like Claude live access to Binance Spot market data and a built-in **multi-timeframe technical-analysis engine**. It runs on **Cloudflare Workers**, at the edge, with no servers to manage.

> **Live endpoint:** `https://binance-live-mcp.binance-live-mcp.workers.dev/mcp`
> Health check: [`/`](https://binance-live-mcp.binance-live-mcp.workers.dev/) · Sample analysis: [`/debug/analysis`](https://binance-live-mcp.binance-live-mcp.workers.dev/debug/analysis)

---

## What it does

Ask Claude *"Analyse BTCUSDT for a spot entry"* and the server:

1. Fetches 5m / 15m / 1h / 4h candles, the 24h ticker and the order book from Binance **in parallel**.
2. Computes **EMA 9/21/50/200, RSI 6/14, MACD, Bollinger Bands, ATR**, support/resistance, volume ratio and order-book imbalance.
3. Scores bullish and bearish signals and checks that **all four timeframes agree**.
4. Builds a conservative trade plan with a **pullback entry, ATR- and structure-based stop-loss, TP1/TP2, and risk/reward**. It returns **WAIT** whenever risk/reward, resistance room or order-book pressure fails its threshold.
5. Returns structured JSON with a confidence score and human-readable reasoning.

It is **read-only by design**. It uses no API keys, has no account access and cannot execute trades.

## Tools (10)

| Tool | Description |
|---|---|
| `binance_price` | Current spot price for a symbol |
| `binance_24hr` | 24-hour rolling statistics |
| `binance_klines` | Candlestick (OHLCV) data, 1s → 1M intervals |
| `binance_order_book` | Order-book depth (5–1000 levels) |
| `binance_book_ticker` | Best bid / ask |
| `binance_recent_trades` | Recent public trades |
| `binance_exchange_info` | Symbol rules and filters |
| `binance_server_time` | Binance server time |
| `binance_ping` | Connectivity check |
| `binance_trade_analysis` | Full multi-timeframe technical analysis and trade plan |

## Architecture

```
Claude / any MCP client
        │  Streamable HTTP (JSON-RPC)
        ▼
Cloudflare Worker  ──►  /mcp   (MCP server, Zod-validated tools)
        │
        ├──► Binance Spot public REST API (primary)
        ├──► Binance.US (same API; automatic failover for US-based callers)
        └──► CoinGecko (price-only, last resort)
```

**Tech stack:** TypeScript · Cloudflare Workers · `@modelcontextprotocol/server` · Cloudflare `agents` MCP handler · Zod

**Engineering highlights**
- Strict input validation on every tool: symbol format, interval enum and limit ranges.
- Parallel upstream requests, so a full four-timeframe analysis finishes in about 2 seconds.
- Geo-aware failover: Binance blocks US callers (HTTP 451), including Claude.ai's servers. The worker automatically retries on Binance.US, which uses the same API, so all tools keep working. Every response says which exchange served it, and the server never fabricates data.
- Clear error reporting. An invalid symbol is returned as a proper MCP tool error.
- CORS and host allow-listing, so browser-based MCP clients can connect.

## Connect it to Claude

**Claude.ai / Claude Desktop:** go to *Settings → Connectors → Add custom connector* and paste:

```
https://binance-live-mcp.binance-live-mcp.workers.dev/mcp
```

**Claude Code:**

```bash
claude mcp add --transport http binance https://binance-live-mcp.binance-live-mcp.workers.dev/mcp
```

Then try asking:
- "What's the price of ETHUSDT right now?"
- "Run a trade analysis on SOLUSDT using 15m entries and the 4h trend."
- "Show me the BTCUSDT order book. Is it bid- or ask-heavy?"

## Run locally

```bash
npm install
npm run dev        # http://localhost:8787/mcp
npm run typecheck
npm run deploy     # deploy to your Cloudflare account
```

## Disclaimer

This tool provides technical analysis for informational purposes only. It is **not financial advice** and executes no trades.
