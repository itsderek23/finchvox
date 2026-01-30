# Changelog

All notable changes to **Finchvox** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.9] - 2026-1-30

- Add error visibility improvements: error icons on spans with exceptions and ability to jump from log entries to their associated span.
- Add tool call visibility: tool icons on LLM spans and tool call names shown in the detail panel.
- Improve trace view readability with larger bars and fonts, better spacing between spans, and improved label overlap detection.

## [0.0.8] - 2026-1-27

- Add anonymous telemetry collection (can be disabled)

## [0.0.7] - 2026-1-25

- Add metrics view to session detail with a breakdown of TTFB by service (LLM, SST, TTS).

## [0.0.6] - 2026-1-20

- Show conversation STT / TTS messages prior to turn completion.
- By default, include modules from the project directory in log capture.
- Cap Pipecat's audio recorder silence insertion to 60 seconds max to prevent large audio file generation.

## [0.0.5] - 2026-1-19

- Add conversation section to session detail view to see a transcript of user <-> agent interactions.
- Add logs to raw data view.

## [0.0.4] - 2026-1-18

- Renames traces to sessions and make logs the default view

## [0.0.3] - 2-26-1-16

- Add log collection support for the standard logger and Loguru.
- Convert the trace view to a swimlane-style vs waterfall to make it easier to see how each service performs throughout the session.

## [0.0.2] - 2026-1-7

- Streamlined installation process with `finchvox.init()` and `FinchvoxProcessor`.

## [0.0.1] - 2026-1-4

Initial public release.
