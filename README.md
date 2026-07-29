> **Public portfolio snapshot:** This repository is a sanitized export of selected components from a larger private production system developed between 2023 and 2026. Original Git history, production credentials, user information, private endpoints, proprietary logic, restricted integrations, internal infrastructure, and some newer revisions are intentionally excluded. The public commit demonstrates selected architecture and implementation work; it does not represent the original development timeline or a complete production deployment.

# SSE Signal Server

[![CI](https://github.com/kinglingo3281/pipelineSSE/actions/workflows/ci.yml/badge.svg)](https://github.com/kinglingo3281/pipelineSSE/actions/workflows/ci.yml)

Real-time Server-Sent Events (SSE) server that aggregates trading signals from multiple pipelines and streams them to connected clients.

## Architecture Overview

This server sits at the center of a multi-pipeline trading signal system:

```
┌─────────────┐     webhook      ┌──────────────┐     SSE stream     ┌───────────────────┐
│  Signal      │ ──────────────> │              │ ──────────────────> │                   │
│  Pipeline    │  /api/v3-signal │   SSE Server │                    │  Trading Terminal  │
│  (autoZAP)   │                 │   (this repo) │                    │  (cryptoterminal)  │
└─────────────┘                  │              │                    │                   │
                                 │  - Express    │                    │  - Next.js app     │
┌─────────────┐   file watcher   │  - PostgreSQL │                    │  - Hyperliquid     │
│  Analysis    │ ──────────────> │  - Chokidar  │                    │  - Auto-trade bots │
│  Pipeline    │  JSON files     │  - Winston   │                    │                   │
│  (analysis)  │                 │              │                    └───────────────────┘
└─────────────┘                  └──────────────┘
```

### Data Sources

1. **Analysis Pipeline** — Writes enhanced analysis JSON files to a watched directory. The `EnhancedDataWatcher` (chokidar) picks up new files, parses them, and inserts the data into PostgreSQL. This data is then broadcast to all connected SSE clients.

2. **Signal Pipeline (autoZAP)** — A standalone Python signal engine that generates trading signals using CCXT + pandas. Signals are delivered via webhook POST to this server's `/api/v3-signal` endpoint, stored in the database, and broadcast to clients.

### Client

The **Trading Terminal (cryptoterminal)** connects to this server via SSE (`/events` endpoint) and receives a unified real-time stream of all signal types. It uses this data to power its Alpha Dashboard, auto-trade bots, and signal display.

## Components

| File | Purpose |
|------|---------|
| `start-sse-server.bat/.sh` | Entry point — starts ngrok tunnel + server wrapper |
| `sse-server-wrapper.js` | Process manager with auto-restart, scheduled restarts, and cleanup |
| `electron-client/src/backend/pure-sse-server.js` | Main Express SSE server (auth, CORS, compression, broadcasting) |
| `enhanced-data-watcher.js` | Chokidar file watcher — ingests analysis JSON into PostgreSQL |
| `json-cleanup-service.js` | Periodic cleanup of old analysis JSON files |
| `setup-database.js` | PostgreSQL schema setup utility |
| `ecosystem.config.js` | PM2 process manager configuration |
| `monitoring/` | Logger (winston), memory watchdog, metrics collector, Telegram alerts, admin dashboard |

## Setup

### Prerequisites

- Node.js >= 16
- PostgreSQL
- ngrok (for tunnel to public URL)

### Installation

```bash
npm install
```

### Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Key settings:
- `DATABASE_URL` — PostgreSQL connection string
- `CORS_ORIGIN` — Comma-separated allowed origins for your trading terminal
- `SSE_MASTER_KEY` — Authentication key for SSE clients
- `VISUALIZATIONS_DIR` — Directory where the analysis pipeline writes JSON files
- `TRADE_SIGNALS_DIR` — Directory for trade signal data

### Database Setup

```bash
node setup-database.js
```

### Running

**Development:**
```bash
node sse-server-wrapper.js
```

**Production (PM2):**
```bash
pm2 start ecosystem.config.js
```

**With ngrok tunnel:**
```bash
# Windows
start-sse-server.bat

# Linux
./start-sse-server.sh
```

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/events` | GET (SSE) | Main SSE stream — clients connect here |
| `/health` | GET | Health check |
| `/api/v3-signal` | POST | Webhook endpoint for external signal pipelines |
| `/admin/dashboard` | GET | Monitoring dashboard UI |
| `/admin/metrics` | GET | JSON metrics snapshot |

## Monitoring

- **Admin Dashboard** — Web UI at `/admin/dashboard` showing connections, memory, broadcast stats
- **Memory Watchdog** — Monitors heap usage, triggers GC, alerts on thresholds
- **Telegram Alerts** — Optional alerts for critical events (configure `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env`)
- **Winston Logging** — Rotating file + console logs with configurable levels
