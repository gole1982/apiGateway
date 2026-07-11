# API Gateway

Resilient Multi-LLM API Gateway for Windows with automatic failover and dynamic token refresh.

## Overview

A robust Go-based API Gateway designed specifically for Windows environments. It provides:

- **Multi-Channel Support**: Manage multiple LLM API providers simultaneously
- **Intelligent Failover**: Automatic failover based on channel priority
- **Dynamic Token Refresh**: Automatic and on-demand token refresh for channels
- **OpenAI-Compatible Proxy**: Drop-in replacement for OpenAI API clients
- **Web Dashboard**: User-friendly interface for channel management
- **Windows Service Integration**: Run as a Windows service or console app
- **System Tray Integration**: Native Windows notifications

## Tech Stack

- **Language**: Go 1.21+ (58.4% of codebase)
- **Frontend**: HTML (41.4% of codebase)
- **Database**: SQLite
- **Deployment**: Docker support (Dockerfile included)
- **Key Dependencies**:
  - `github.com/google/uuid` - UUID generation
  - `gopkg.in/ini.v1` - Configuration management
  - `modernc.org/sqlite` - SQLite driver

## Quick Start

### Installation

```powershell
# Install as Windows Service
.\gateway.exe -install

# Or run in console mode
.\gateway.exe
```

### Service Management

```powershell
.\gateway.exe -install       # Install service
.\gateway.exe -uninstall     # Remove service
sc query GatewayService      # Check service status
sc start GatewayService      # Start service
sc stop GatewayService       # Stop service
```

## Ports

- **Proxy**: `13579` - OpenAI-compatible `/v1/chat/completions` endpoint
- **Dashboard**: `24680` - Web admin UI

## Configuration

Create `proxy.cfg` in the same directory as `gateway.exe`:

```ini
proxy_port = 13579
web_port = 24680
refresh_interval_sec = 600
```

### Configuration Parameters

| Key | Default | Description |
|-----|---------|-------------|
| `proxy_port` | 13579 | OpenAI-compatible proxy port |
| `web_port` | 24680 | Dashboard web port |
| `refresh_interval_sec` | 600 | Token refresh interval in seconds (10 minutes) |

## Channel Management

### Web Dashboard

Access the dashboard at `http://localhost:24680` to manage channels.

1. Click **+ Add Channel**
2. Fill in the channel details:
   - **Alias**: Channel name (e.g., `openai-gpt4`)
   - **API URL**: Upstream API endpoint
   - **Token / API Key**: Your API key
   - **Priority**: Lower number = higher priority for failover
   - **Dynamic Token**: Enable auto token refresh
   - **Token Command**: Command to fetch new token (if dynamic enabled)

## Dynamic Token Refresher

For channels with `is_dynamic = true`, the gateway provides:

1. **Background Refresh**: Periodically executes `token_command` to fetch fresh tokens
   - Interval controlled by `refresh_interval_sec`
   - Runs in background without interrupting service
   
2. **401 Recovery**: On HTTP 401 response
   - Instantly re-fetches token using `token_command`
   - Retries the original request automatically
   - Client connection remains alive during refresh

## API Endpoint

### OpenAI-Compatible Chat Completions

```
POST http://localhost:13579/v1/chat/completions
```

The gateway proxies requests to configured channels with automatic failover:

```bash
curl -X POST http://localhost:13579/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

## Build

### Build from Source

```powershell
# Build gateway service executable
go build -o gateway.exe ./cmd/gateway

# Build system tray application
go build -o gateway_tray.exe ./cmd/tray
```

### Docker Build

```dockerfile
# See Dockerfile for containerized deployment
docker build -t apigateway .
```

## Architecture

```
gateway.exe (Windows Service / Console App)
    ├── Config (proxy.cfg) 
    │   └── Ports, refresh interval settings
    │
    ├── SQLite Database (gateway.db)
    │   └── Channel configuration storage
    │
    ├── Proxy Handler
    │   └── OpenAI-compatible /v1/chat/completions with failover logic
    │
    ├── Token Refresher
    │   ├── Background token refresh worker
    │   └── 401 HTTP response recovery handler
    │
    └── IPC Notifier
        └── TCP communication to gateway_tray.exe for notifications

gateway_tray.exe (User Session - Optional)
    └── Windows Toast Notifications
        └── Service events and alerts
```

## Channel Priority & Failover

Channels are evaluated in priority order (lower number = higher priority):

1. Request goes to highest priority channel
2. On success (2xx response) → request complete
3. On failure → try next priority channel
4. On HTTP 401 → refresh token and retry same channel
5. If all channels fail → return error to client

## Database

The gateway uses SQLite (`gateway.db`) to persist channel configurations:

- Channel aliases, API URLs, tokens
- Priority settings
- Dynamic token refresh configurations
- Token command definitions

## Windows Integration

### System Tray (gateway_tray.exe)

Optional companion application that:
- Displays notifications from the gateway service
- Shows service status
- Allows quick service start/stop (when running as Windows Service)

## Troubleshooting

### Service Won't Start

```powershell
# Check Windows Event Viewer for errors
# Or check if port 13579/24680 are in use
netstat -ano | findstr ":13579"
netstat -ano | findstr ":24680"
```

### Dynamic Token Refresh Not Working

1. Verify `token_command` is valid
2. Check `refresh_interval_sec` in `proxy.cfg`
3. Ensure token command has proper permissions
4. Check gateway logs for command execution errors

### Connection to Upstream API Fails

1. Verify **API URL** is correct
2. Test API key manually
3. Check firewall rules allow outbound connections
4. Verify **Priority** settings for failover order

## Contributing

Contributions are welcome! Areas for enhancement:
- Additional proxy endpoint support
- More token refresh strategies
- Dashboard UI improvements
- Cross-platform support

## License

[Add your license information here]

## Version

Current version: 1.0  
Go: 1.21+  
Tested on: Windows 10/11
