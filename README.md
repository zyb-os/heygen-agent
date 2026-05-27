# heygen-agent

HeyGen MCP agent for the ZybOS orchestrator. Exposes AI avatar video generation, status polling, asset discovery, and account management via the [HeyGen v2 API](https://docs.heygen.com).

## Capabilities

| Capability | Description |
|---|---|
| `create_video` | Generate an AI avatar video from a text script |
| `get_video_status` | Poll the status of a video generation job |
| `download_video` | Get the download URL for a completed video |
| `list_videos` | List previously generated videos |
| `delete_video` | Delete a video by ID |
| `list_avatars` | List all available avatars |
| `list_voices` | List TTS voices (filterable by language / gender) |
| `list_templates` | List available video templates |
| `create_video_from_template` | Generate a video using a template |
| `get_remaining_quota` | Check remaining API credits |

## Setup

### 1. Get a HeyGen API key

Log in to [app.heygen.com](https://app.heygen.com) → Settings → API → copy your key.

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run

```bash
# API key via env var
HEYGEN_API_KEY=your_key python main.py --orchestrator-url http://localhost:8000

# Or let the orchestrator push the key via required_settings
python main.py --orchestrator-url http://localhost:8000
```

The agent registers itself with the orchestrator on startup and reconnects automatically on disconnect.

## Video generation workflow

```
1. list_avatars          → pick an avatar_id
2. list_voices           → pick a voice_id  (optional)
3. create_video          → returns video_id
4. get_video_status      → poll until status == "completed"
5. download_video        → returns download_url
```

## Environment variables

| Variable | Description |
|---|---|
| `HEYGEN_API_KEY` | HeyGen API key (can also be set via orchestrator settings) |
| `ORCHESTRATOR_URL` | Orchestrator base URL (default: `http://localhost:8000`) |
| `LOG_LEVEL` | Logging verbosity: `DEBUG`, `INFO`, `WARNING` (default: `INFO`) |

## Files

| File | Purpose |
|---|---|
| `main.py` | Entry point and CLI argument parsing |
| `orchestrator_client.py` | Orchestrator WebSocket protocol, task routing |
| `heygen_client.py` | Async HeyGen REST API client |
| `requirements.txt` | Python dependencies |
