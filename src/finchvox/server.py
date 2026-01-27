"""
Unified FinchVox server combining collector and UI functionality.

This module provides a single server that handles both data collection
(audio, logs, exceptions, OTLP traces) and the web UI for viewing traces.
"""

import asyncio
import signal
import grpc
import uvicorn
from concurrent import futures
from pathlib import Path
from fastapi import FastAPI
from loguru import logger
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import (
    add_TraceServiceServicer_to_server
)
from opentelemetry.proto.collector.logs.v1.logs_service_pb2_grpc import (
    add_LogsServiceServicer_to_server
)

from finchvox.collector.service import TraceCollectorServicer
from finchvox.collector.log_service import LogCollectorServicer
from finchvox.collector.writer import SpanWriter
from finchvox.collector.log_writer import LogWriter
from finchvox.collector.audio_handler import AudioHandler
from finchvox.collector.collector_routes import register_collector_routes
from finchvox.ui_routes import register_ui_routes
from finchvox.collector.config import (
    GRPC_PORT,
    MAX_WORKERS,
    get_default_data_dir,
    get_sessions_base_dir
)
from finchvox import telemetry


class UnifiedServer:
    """
    Unified server managing both gRPC (OTLP traces) and HTTP (collector + UI).

    This server provides:
    - gRPC endpoint for OpenTelemetry trace collection (default port 4317)
    - HTTP endpoints for audio/logs/exceptions collection (under /collector prefix)
    - Web UI and REST API for viewing traces (at root /)
    """

    def __init__(self, port: int = 3000, grpc_port: int = GRPC_PORT, host: str = "0.0.0.0", data_dir: Path = None):
        """
        Initialize the unified server.

        Args:
            port: HTTP server port (default: 3000)
            grpc_port: gRPC server port (default: 4317)
            host: Host to bind to (default: "0.0.0.0")
            data_dir: Base data directory (default: ~/.finchvox)
        """
        self.port = port
        self.grpc_port = grpc_port
        self.host = host
        self.data_dir = data_dir if data_dir else get_default_data_dir()

        # Initialize shared writer instances
        self.span_writer = SpanWriter(self.data_dir)
        self.log_writer = LogWriter(self.data_dir)
        self.audio_handler = AudioHandler(self.data_dir)

        # Server instances
        self.grpc_server = None
        self.http_server = None
        self.shutdown_event = asyncio.Event()
        self._is_shutting_down = False

        # Create unified FastAPI app
        self.app = self._create_app()

    def _create_app(self) -> FastAPI:
        """
        Create unified FastAPI application with both collector and UI routes.

        Returns:
            Configured FastAPI application
        """
        app = FastAPI(
            title="FinchVox Unified Server",
            description="Combined collector and UI server for voice AI observability",
            version="0.1.0",
        )

        # Register UI routes first (includes static file mounts)
        register_ui_routes(app, self.data_dir)

        # Register collector routes with /collector prefix
        register_collector_routes(
            app,
            self.audio_handler,
            prefix="/collector"
        )

        return app

    async def start_grpc(self):
        """Start the gRPC server for OTLP trace collection."""
        logger.info(f"Starting OTLP gRPC collector on port {self.grpc_port}")

        # Create gRPC server with thread pool
        self.grpc_server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
        )

        # Register trace service
        trace_servicer = TraceCollectorServicer(self.span_writer)
        add_TraceServiceServicer_to_server(trace_servicer, self.grpc_server)

        # Register logs service
        log_servicer = LogCollectorServicer(self.log_writer)
        add_LogsServiceServicer_to_server(log_servicer, self.grpc_server)

        # Bind to port (insecure for PoC - no TLS)
        self.grpc_server.add_insecure_port(f'[::]:{self.grpc_port}')

        # Start serving
        self.grpc_server.start()
        logger.info(f"OTLP collector listening on port {self.grpc_port}")
        logger.info(f"Writing sessions to: {get_sessions_base_dir(self.data_dir).absolute()}")

    async def start_http(self):
        """Start the HTTP server using uvicorn."""
        logger.info(f"Starting HTTP server on {self.host}:{self.port}")

        # Configure uvicorn server
        config = uvicorn.Config(
            self.app,
            host=self.host,
            port=self.port,
            log_level="info",
            access_log=True,
        )
        self.http_server = uvicorn.Server(config)

        logger.info(f"HTTP server listening on http://{self.host}:{self.port}")
        logger.info(f"  - UI: http://{self.host}:{self.port}")
        logger.info(f"  - Collector: http://{self.host}:{self.port}/collector")
        logger.info(f"Data directory: {self.data_dir.absolute()}")

        # Run server until shutdown event
        await self.http_server.serve()

    async def start(self):
        """Start both gRPC and HTTP servers concurrently."""
        telemetry.send_event("server_start", dedupe=True)

        await self.start_grpc()

        await self.start_http()

    async def stop(self, grace_period: int = 5):
        """
        Gracefully stop both servers.

        Args:
            grace_period: Seconds to wait for in-flight requests to complete
        """
        # Prevent multiple shutdown attempts
        if self._is_shutting_down:
            return

        self._is_shutting_down = True
        logger.info(f"Shutting down servers (grace period: {grace_period}s)")

        # Stop HTTP server
        if self.http_server:
            logger.info("Stopping HTTP server...")
            self.http_server.should_exit = True
            await asyncio.sleep(0.1)  # Give it time to process shutdown

        # Stop gRPC server
        if self.grpc_server:
            logger.info("Stopping gRPC server...")
            self.grpc_server.stop(grace_period)

        logger.info("All servers stopped")

    def run(self):
        """
        Blocking entry point for running the unified server.

        Sets up signal handlers and runs the event loop until shutdown.
        """
        async def run_with_signals():
            loop = asyncio.get_running_loop()

            def handle_shutdown(signum):
                if not self._is_shutting_down:
                    logger.info(f"Received signal {signum}")
                    # Remove signal handlers to prevent duplicate calls
                    for sig in (signal.SIGINT, signal.SIGTERM):
                        loop.remove_signal_handler(sig)
                    # Create shutdown task
                    asyncio.create_task(self.stop())

            # Register signal handlers
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, lambda s=sig: handle_shutdown(s))

            try:
                await self.start()
            except (KeyboardInterrupt, asyncio.CancelledError):
                await self.stop()

        asyncio.run(run_with_signals())
