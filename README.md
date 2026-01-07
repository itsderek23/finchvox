# <img src="ui/images/finchvox-logo.png" height=24 /> Finchvox - elevated debuggability for Voice AI apps

Do your eyes bleed like a Vecna victim watching Pipecat logs fly by? Do OpenTelemetry traces look impressive … yet explain nothing? If so, meet Finchvox, a local debuggability tool purpose-built for Voice AI apps. 

Finchvox unifies conversation audio and traces in a single UI, highlighting voice-specific problems like interruptions and high user <-> bot latency. Good luck convincing DataDog to add that!

_👇 Click the image for a short video:_
<a href="https://raw.githubusercontent.com/itsderek23/finchvox/refs/heads/main/docs/demo.gif" target="_blank"><img src="./docs/screenshot.png" /></a>

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Setup](#setup)
- [Usage](#usage---finchvox-server)
- [Troubleshooting](#troubleshooting)

## Prerequisites

- Python 3.10 or higher
- A Pipecat Voice AI application

## Installation

```bash
# uv
uv add finchvox "pipecat-ai[tracing]"

# Or with pip
pip install finchvox "pipecat-ai[tracing]"
```

## Setup

1. Add the following to the top of your bot (e.g., `bot.py`):

```python
import finchvox
from finchvox import FinchvoxProcessor

finchvox.init(service_name="my-voice-app")
```

2. Add `FinchvoxProcessor` to your pipeline, ensuring it comes after `transport.output()`:

```python
pipeline = Pipeline([
    # SST, LLM, TTS, etc. processors
    transport.output(),
    FinchvoxProcessor(), # Must come after transport.output()
    context_aggregator.assistant(),
])
```

3. Initialize your `PipelineTask` with metrics, tracing and turn tracking enabled:

```python
task = PipelineTask(
    pipeline,
    params=PipelineParams(enable_metrics=True),
    enable_tracing=True,
    enable_turn_tracking=True,
)
```

## Usage - Finchvox server

```bash
uv run finchvox start
```

For the list of available options, run:

```bash
uv run finchvox --help
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
