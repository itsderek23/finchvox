"""
Audio recording with chunked stereo capture and timing metadata.

Records conversation audio with:
- Stereo format: user audio on left channel, bot audio on right channel
- Chunked recording every 5-10 seconds for continuous streaming
- Timing events for latency calculation
- Association with OpenTelemetry trace IDs
- Direct upload to configured endpoint (no local file storage)
"""

import asyncio
import io
import json
import wave
from datetime import datetime
from typing import Optional

import aiohttp
from loguru import logger
from opentelemetry import trace
from pipecat.processors.audio.audio_buffer_processor import AudioBufferProcessor
# Add Pipecat's conversation context provider
from pipecat.utils.tracing.conversation_context_provider import (
    ConversationContextProvider
)


class ConversationAudioRecorder:
    """
    Records conversation audio with timing metadata for latency analysis.

    Audio chunks are uploaded directly to a configured endpoint with no local file storage.
    """

    def __init__(
        self,
        chunk_duration_seconds: int = 5,
        sample_rate: int = 16000,
        endpoint: Optional[str] = "http://localhost:3000",
    ):
        """
        Initialize audio recorder.

        Args:
            chunk_duration_seconds: Duration of each audio chunk (5-10 seconds recommended)
            sample_rate: Audio sample rate (default 16kHz)
            endpoint: URL of finchvox HTTP server (default: "http://localhost:3000")
                     If None, recording will fail when started
        """
        self.chunk_duration = chunk_duration_seconds
        self.sample_rate = sample_rate
        self.endpoint = endpoint

        # Create AudioBufferProcessor with stereo configuration
        self.audio_buffer = AudioBufferProcessor(
            sample_rate=self.sample_rate,  # Explicit sample rate (16000 Hz)
            num_channels=2,                # Stereo: user left, bot right
            buffer_size=320000,            # ~10 seconds at 16kHz, 16-bit
            enable_turn_audio=False,       # Continuous recording, not per-turn
        )

        # Timing events for latency calculation
        self.timing_events = []
        self.current_trace_id: Optional[str] = None
        self.conversation_start_time: Optional[datetime] = None
        self.chunk_counter = 0

        self._setup_event_handlers()

    def _setup_event_handlers(self):
        """Set up audio buffer event handlers for chunked recording."""

        @self.audio_buffer.event_handler("on_audio_data")
        async def on_audio_data(buffer, audio, sample_rate, num_channels):
            """Handle audio data chunks (called every chunk_duration seconds)."""
            try:
                # Get trace ID from Pipecat's conversation context provider
                trace_id = None

                # First, try to get trace_id from active conversation span
                context_provider = ConversationContextProvider.get_instance()
                conversation_context = context_provider.get_current_conversation_context()

                if conversation_context:
                    # Extract span context from conversation context
                    span = trace.get_current_span(conversation_context)
                    span_context = span.get_span_context()
                    if span_context.trace_id != 0:
                        trace_id = format(span_context.trace_id, "032x")

                # Fallback to manually set trace_id (for backwards compatibility)
                if not trace_id:
                    trace_id = self.current_trace_id or "no_trace"

                # Prepare metadata
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                metadata = {
                    "trace_id": trace_id,
                    "chunk_number": self.chunk_counter,
                    "timestamp": timestamp,
                    "sample_rate": sample_rate,
                    "num_channels": num_channels,
                    "channels": {
                        "0": "user",
                        "1": "bot"
                    },
                    "timing_events": self.timing_events,
                    "conversation_start": (
                        self.conversation_start_time.isoformat()
                        if self.conversation_start_time
                        else None
                    ),
                }

                # Upload to endpoint
                upload_success = await self.upload_chunk(
                    trace_id=trace_id,
                    chunk_number=self.chunk_counter,
                    audio_data=audio,
                    metadata=metadata
                )

                if upload_success:
                    logger.info(
                        f"Uploaded audio chunk {self.chunk_counter} for trace {trace_id[:8]}... "
                        f"({len(self.timing_events)} timing events)"
                    )
                else:
                    logger.error(
                        f"Failed to upload chunk {self.chunk_counter} for trace {trace_id[:8]}..."
                    )

                self.chunk_counter += 1

                # Clear old timing events from previous chunks (keep recent for context)
                if len(self.timing_events) > 100:
                    self.timing_events = self.timing_events[-50:]

            except Exception as e:
                logger.error(f"Failed to process audio chunk: {e}", exc_info=True)

    async def upload_chunk(
        self,
        trace_id: str,
        chunk_number: int,
        audio_data: bytes,
        metadata: dict
    ) -> bool:
        """
        Upload audio chunk to endpoint via HTTP POST.

        Args:
            trace_id: OpenTelemetry trace ID
            chunk_number: Sequential chunk number
            audio_data: Raw audio bytes (WAV format)
            metadata: Metadata dictionary

        Returns:
            True if upload succeeded, False otherwise
        """
        try:
            url = f"{self.endpoint}/collector/audio/{trace_id}/chunk"

            # Create WAV file in memory
            wav_buffer = io.BytesIO()
            with wave.open(wav_buffer, 'wb') as wav_file:
                wav_file.setnchannels(metadata['num_channels'])
                wav_file.setsampwidth(2)  # 16-bit audio
                wav_file.setframerate(metadata['sample_rate'])
                wav_file.writeframes(audio_data)

            wav_buffer.seek(0)

            async with aiohttp.ClientSession() as session:
                form = aiohttp.FormData()
                form.add_field(
                    'audio',
                    wav_buffer,
                    filename=f"chunk_{chunk_number:04d}.wav",
                    content_type='audio/wav'
                )
                form.add_field(
                    'metadata',
                    json.dumps(metadata),
                    content_type='application/json'
                )

                # Upload with timeout
                async with session.post(
                    url,
                    data=form,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 201:
                        result = await response.json()
                        logger.debug(
                            f"Uploaded chunk {chunk_number} for trace {trace_id[:8]}... "
                            f"to endpoint: {result.get('file_path')}"
                        )
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(
                            f"Failed to upload chunk {chunk_number}: "
                            f"HTTP {response.status}: {error_text}"
                        )
                        return False

        except asyncio.TimeoutError:
            logger.error(f"Timeout uploading chunk {chunk_number} to endpoint")
            return False
        except Exception as e:
            logger.error(
                f"Error uploading chunk {chunk_number} to endpoint: {e}",
                exc_info=True
            )
            return False


    async def start_recording(self, trace_id: Optional[str] = None):
        """
        Start recording audio for a conversation.

        Args:
            trace_id: Optional trace ID hint. If not provided or unavailable,
                      the recorder will automatically extract trace_id from
                      the active conversation span during chunk capture.

        Raises:
            ValueError: If endpoint is not configured
        """
        if not self.endpoint:
            raise ValueError(
                "Cannot start recording: endpoint is not configured. "
                "Provide an endpoint URL when initializing ConversationAudioRecorder."
            )

        self.current_trace_id = trace_id
        self.conversation_start_time = datetime.now()
        self.chunk_counter = 0
        self.timing_events = []

        await self.audio_buffer.start_recording()
        logger.info(f"Started audio recording for trace {trace_id}")

    async def stop_recording(self):
        """Stop recording audio."""
        await self.audio_buffer.stop_recording()
        logger.info(
            f"Stopped audio recording. Captured {self.chunk_counter} chunks "
            f"with {len(self.timing_events)} timing events"
        )

    def add_timing_event(self, event_type: str, metadata: dict = None):
        """
        Add a timing event for latency calculation.

        Args:
            event_type: Type of event (e.g., 'user_stopped', 'bot_started', 'bot_stopped')
            metadata: Additional metadata for the event
        """
        event = {
            "type": event_type,
            "timestamp": datetime.now().isoformat(),
            "relative_time": (
                (datetime.now() - self.conversation_start_time).total_seconds()
                if self.conversation_start_time
                else 0
            ),
            "metadata": metadata or {},
        }
        self.timing_events.append(event)
        logger.debug(f"Timing event: {event_type}")

    def get_processor(self) -> AudioBufferProcessor:
        """Get the AudioBufferProcessor to add to pipeline."""
        return self.audio_buffer
