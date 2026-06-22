# API Gateway

Resilient Multi-LLM API Gateway for Windows with automatic failover and dynamic token refresh.

## Quick Start

```powershell
# Install as Windows Service
.\gateway.exe -install

# Or run in console mode
.\gateway.exe
```

## Ports

- **Proxy**: `13579` - OpenAI-compatible `/v1/chat/completions`
- **Dashboard**: `24680` - Web admin UI

## Configuration

Create `proxy.cfg` in the same directory as `gateway.exe`:

```ini
proxy_port = 13579
web_port = 24680
refresh_interval_sec = 600
```

### Global Settings

| Key | Default | Description |
|-----|---------|-------------|
| `proxy_port` | 13579 | OpenAI-compatible proxy port |
| `web_port` | 24680 | Dashboard web port |
| `refresh_interval_sec` | 600 | Token refresh interval (seconds) |

## Channel Management

Channels are managed via the **Web Dashboard** at `http://localhost:24680`.

1. Click **+ Add Channel**
2. Fill in the channel details:
   - **Alias**: Channel name (e.g., `openai-gpt4`)
   - **API URL**: Upstream API endpoint
   - **Token / API Key**: Your API key
   - **Priority**: Lower number = higher priority
   - **Dynamic Token**: Enable auto token refresh
   - **Token Command**: Command to fetch new token (if dynamic)

## Dynamic Token Refresher

For channels with `is_dynamic = true`, the gateway:

1. **Background Refresh** - Periodically executes `token_command` to fetch fresh tokens
2. **401 Recovery** - On HTTP 401, instantly re-fetches token and retries request

The client connection remains alive during token refresh.

## Build

```powershell
go build -o gateway.exe ./cmd/gateway
go build -o gateway_tray.exe ./cmd/tray
```

## Service Management

```powershell
.\gateway.exe -install   # Install service
.\gateway.exe -uninstall # Remove service
sc query GatewayService   # Check status
```

## Architecture

```
gateway.exe (Windows Service / Console App)
    ├── Config (proxy.cfg) - Ports, refresh interval
    ├── SQLite (gateway.db) - Channel storage
    ├── Proxy Handler - /v1/chat/completions with failover
    ├── Token Refresher - Dynamic token management
    └── Notify IPC - TCP to gateway_tray.exe

gateway_tray.exe (User Session)
    └── Windows Toast Notifications
```
