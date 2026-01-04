# Finchvox Tracing for Pipecat

This demo showcases OpenTelemetry tracing integration + audio recordings for Pipecat services using Finchvox.

## Setup Instructions

### 1. Install and start Finchvox

See the main [README](../../README.md) for installation instructions.

### 2. Environment Configuration

Create a `.env` file with your API keys and enable tracing:

```
ENABLE_TRACING=true
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317  # Point to your Jaeger backend
# OTEL_CONSOLE_EXPORT=true  # Set to any value for debug output to console

# Service API keys
DEEPGRAM_API_KEY=your_key_here
CARTESIA_API_KEY=your_key_here
OPENAI_API_KEY=your_key_here
```

### 3. Setup venv and install Dependencies

```bash
uv sync
```

> Install only the grpc exporter. If you have a conflict, uninstall the http exporter.

### 4. Run the Demo

```bash
uv run bot.py
```

### 5. View Traces in Finchvox

Open your browser to [http://localhost:3000](http://localhost:3000) and view traces.
