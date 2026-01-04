#!/usr/bin/env python3
"""
Simple script to send test spans to the Finchvox collector.
Run this while the collector is running to verify it works.
"""

import time
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

# Configure resource
resource = Resource.create({
    "service.name": "test-voice-app",
    "service.version": "1.0.0"
})

# Create tracer provider
provider = TracerProvider(resource=resource)
trace.set_tracer_provider(provider)

# Configure OTLP exporter pointing to Finchvox
exporter = OTLPSpanExporter(
    endpoint="http://localhost:4317",
    insecure=True
)

# Add span processor
provider.add_span_processor(BatchSpanProcessor(exporter))

# Get tracer
tracer = trace.get_tracer(__name__)

print("Sending test spans to Finchvox collector at localhost:4317...")

# Test 1: Single span
print("\n1. Sending a single test span...")
with tracer.start_as_current_span("test-span-1") as span:
    span.set_attribute("test.type", "single")
    span.set_attribute("http.method", "GET")
    time.sleep(0.1)

# Test 2: Nested spans (parent-child relationship)
print("2. Sending nested spans (parent-child)...")
with tracer.start_as_current_span("parent-span") as parent:
    parent.set_attribute("span.level", "parent")
    time.sleep(0.05)

    with tracer.start_as_current_span("child-span-1") as child1:
        child1.set_attribute("span.level", "child")
        child1.set_attribute("child.id", 1)
        time.sleep(0.05)

    with tracer.start_as_current_span("child-span-2") as child2:
        child2.set_attribute("span.level", "child")
        child2.set_attribute("child.id", 2)
        time.sleep(0.05)

# Test 3: Multiple independent spans (will be in same trace)
print("3. Sending multiple spans in a single trace...")
with tracer.start_as_current_span("voice-conversation") as conv:
    conv.set_attribute("conversation.id", "test-123")

    with tracer.start_as_current_span("stt-process") as stt:
        stt.set_attribute("voice.component", "speech-to-text")
        stt.set_attribute("audio.duration_ms", 2500)
        time.sleep(0.05)

    with tracer.start_as_current_span("llm-process") as llm:
        llm.set_attribute("voice.component", "llm")
        llm.set_attribute("llm.model", "gpt-4")
        llm.set_attribute("llm.tokens", 150)
        time.sleep(0.1)

    with tracer.start_as_current_span("tts-process") as tts:
        tts.set_attribute("voice.component", "text-to-speech")
        tts.set_attribute("audio.output_ms", 3000)
        time.sleep(0.05)

# Force flush to ensure all spans are sent
print("\nFlushing spans to collector...")
provider.force_flush()

print("\n✅ Done! Check the traces/ directory for JSONL files.")
print("   View traces with: cat traces/trace_*.jsonl | jq")
print("   Or: ls -lh traces/")
