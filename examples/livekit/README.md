# LiveKit Agent Example with Finchvox

A minimal voice agent demonstrating Finchvox instrumentation with LiveKit agents.

## Prerequisites

- Python 3.10+
- A running LiveKit server (local or cloud)
- API keys for OpenAI, Deepgram, and Cartesia

## Setup

1. Install dependencies:

```bash
cd /path/to/finchvox
uv pip install -e ".[livekit]"
```

2. Copy the environment template and fill in your API keys:

```bash
cp .env.example .env
```

3. Start the Finchvox server (in a separate terminal):

```bash
uv run finchvox start
```

4. Start the LiveKit agent:

```bash
python agent.py dev
```

## Testing

1. Open the LiveKit playground or connect via a LiveKit client
2. Speak to the agent
3. View traces in the Finchvox UI at http://localhost:3000

## What Gets Traced

With `finchvox.init_livekit()`, you'll see spans for:
- `agent_session` - The overall agent session
- `user_speaking` / `agent_speaking` - Speech turn events
- `llm_node` - LLM inference calls
- `tts_node` - Text-to-speech synthesis
- `stt` - Speech-to-text transcription

## Configuration

The `init_livekit()` function accepts:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `service_name` | `"livekit-agent"` | Service name in traces |
| `endpoint` | `"http://localhost:4317"` | OTLP gRPC endpoint |
| `insecure` | `True` | Use insecure connection |
| `metadata` | `None` | Additional span attributes |

Example with custom metadata:

```python
tracer_provider = finchvox.init_livekit(
    service_name="my-agent",
    metadata={
        "agent.version": "1.0.0",
        "environment": "development",
    },
)
```
