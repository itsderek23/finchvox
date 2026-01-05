# Finchvox Tracing for Pipecat

This demo showcases OpenTelemetry tracing integration + audio recordings for Pipecat apps using Finchvox.

## Setup Instructions

### 1. Install and start Finchvox

See the main [README](../../README.md) for installation instructions.

### 2. Environment Configuration

```
cp env.example .env
```

Update the `.env` file with the required API creds.

### 3. Setup venv and install Dependencies

```bash
uv sync
```

### 4. Run the Demo

```bash
uv run bot.py
```

### 5. View Traces in Finchvox

Open your browser to [http://localhost:3000](http://localhost:3000) and view traces.
