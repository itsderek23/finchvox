# <img src="ui/images/finchvox-logo.png" height=24 /> Finchvox - the local Voice AI debugger

Finchvox makes it easier to understand where your cascading Voice AI pipeline went wrong. It collects conversation audio and traces, presenting them in a single UI.

There are two main components:

1. The Finchvox server - collects OpenTelemetry spans and audio data and serves the Finchvox UI.
2. The Finchvox audio recorder - a Pipecat processor that records conversation audio for each client session.

## Prerequisites

- Python 3.10 or higher
- A Pipecat Voice AI application

## Installation

```bash
# Using uv (recommended)
uv add finchvox "pipecat-ai[tracing]"

# Or with pip (PyPI)
pip install finchvox "pipecat-ai[tracing]"
```

## Usage - Finchvox server

```bash
uv run finchvox start
```

For the list of available options, run:

```bash
uv run finchvox --help
```

## Setup - Enable Tracing in Your Pipecat Application

```python
import os
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from pipecat.utils.tracing.setup import setup_tracing
```

```python
# Step 1: Initialize OpenTelemetry with your chosen exporter
exporter = OTLPSpanExporter(
    endpoint="http://localhost:4317",
    insecure=True,
)

setup_tracing(
    service_name="my-voice-app",
    exporter=exporter,
)

# Step 2: Enable tracing in your PipelineTask
task = PipelineTask(
    pipeline,
    params=PipelineParams(
        enable_metrics=True
    ),
    enable_tracing=True,
    enable_turn_tracking=True
)
```

For the full list of OpenTelemetry setup options, see the [Pipecat OpenTelemetry docs](https://docs.pipecat.ai/server/utilities/opentelemetry#overview).

## Setup - Enable Audio Recording

Import the audio recorder and add it to your pipeline:

```python
from finchvox.audio_recorder import ConversationAudioRecorder
```

```python
audio_recorder = ConversationAudioRecorder()

pipeline = Pipeline(
  [
      # Other processors, like STT, LLM, TTS, etc.
      audio_recorder.get_processor(),
      # context_aggregator.assistant(),
  ]
)
```

Start and stop recording on client connect/disconnect events:

```python
@transport.event_handler("on_client_connected")
async def on_client_connected(transport, client):
    await audio_recorder.start_recording()

    # Other initialization logic...

@transport.event_handler("on_client_disconnected")
async def on_client_disconnected(transport, client):
    await audio_recorder.stop_recording()

    # Other cleanup logic...
```

## Troubleshooting

### Port already in use

If port 4317 is already occupied:

```bash
# Find process using port
lsof -i :4317

# Kill the process
kill -9 <PID>
```

### No spans being written

1. Check collector is running: Look for "OTLP collector listening on port 4317" log message
2. Verify client endpoint: Ensure Pipecat is configured to send to `http://localhost:4317`
