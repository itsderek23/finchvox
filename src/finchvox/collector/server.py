import asyncio
import signal
from concurrent import futures

import grpc
import uvicorn
from loguru import logger
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import (
    add_TraceServiceServicer_to_server,
)

from finchvox.scheduler import start_scheduler, stop_scheduler

from .audio_handler import AudioHandler
from .config import (
    AUDIO_DIR,
    EXCEPTIONS_DIR,
    GRPC_PORT,
    HTTP_PORT,
    LOGS_DIR,
    MAX_WORKERS,
    TRACES_DIR,
    get_default_data_dir,
)
from .exceptions_writer import ExceptionsWriter
from .http_server import create_app
from .logs_writer import LogWriter
from .service import TraceCollectorServicer
from .writer import SpanWriter


class CollectorServer:
    """Manages both gRPC and HTTP server lifecycle."""

    def __init__(self):
        self.grpc_server = None
        self.http_server = None
        self.span_writer = SpanWriter(TRACES_DIR)
        self.log_writer = LogWriter(LOGS_DIR)
        self.exceptions_writer = ExceptionsWriter(EXCEPTIONS_DIR)
        self.audio_handler = AudioHandler(AUDIO_DIR)
        self.shutdown_event = asyncio.Event()

    async def start_grpc(self):
        """Start the gRPC server."""
        logger.info(f"Starting OTLP gRPC collector on port {GRPC_PORT}")

        # Create gRPC server with thread pool
        self.grpc_server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
        )

        # Register our service implementation
        servicer = TraceCollectorServicer(self.span_writer)
        add_TraceServiceServicer_to_server(servicer, self.grpc_server)

        # Bind to port (insecure for PoC - no TLS)
        self.grpc_server.add_insecure_port(f"[::]:{GRPC_PORT}")

        # Start serving
        self.grpc_server.start()
        logger.info(f"OTLP collector listening on port {GRPC_PORT}")
        logger.info(f"Writing traces to: {TRACES_DIR.absolute()}")

    async def start_http(self):
        """Start the HTTP server using uvicorn."""
        logger.info(f"Starting HTTP collector on port {HTTP_PORT}")

        # Create FastAPI app with injected dependencies
        app = create_app(self.audio_handler, self.log_writer, self.exceptions_writer)

        # Configure uvicorn server
        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=HTTP_PORT,
            log_level="info",
            access_log=True,
        )
        self.http_server = uvicorn.Server(config)

        logger.info(f"HTTP collector listening on port {HTTP_PORT}")
        logger.info(f"Writing audio to: {AUDIO_DIR.absolute()}")
        logger.info(f"Writing logs to: {LOGS_DIR.absolute()}")
        logger.info(f"Writing exceptions to: {EXCEPTIONS_DIR.absolute()}")

        # Run server until shutdown event
        await self.http_server.serve()

    async def start(self):
        """Start both servers concurrently."""
        await self.start_grpc()

        data_dir = get_default_data_dir()
        start_scheduler(data_dir)

        await self.start_http()

    async def stop(self, grace_period=5):
        """Gracefully stop both servers."""
        logger.info(f"Shutting down servers (grace period: {grace_period}s)")

        stop_scheduler()

        if self.http_server:
            logger.info("Stopping HTTP server...")
            self.http_server.should_exit = True
            await asyncio.sleep(0.1)  # Give it time to process shutdown

        # Stop gRPC server
        if self.grpc_server:
            logger.info("Stopping gRPC server...")
            self.grpc_server.stop(grace_period)

        logger.info("All servers stopped")


async def run_server_async():
    """Async entry point for running the collector server."""
    server = CollectorServer()

    # Setup signal handlers
    loop = asyncio.get_running_loop()

    def handle_shutdown(signum):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(server.stop())

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: handle_shutdown(s))

    try:
        await server.start()
    except KeyboardInterrupt:
        await server.stop()


def run_server():
    """Entry point for running the collector server (blocks until shutdown)."""
    asyncio.run(run_server_async())
