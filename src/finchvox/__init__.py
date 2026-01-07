from loguru import logger

_initialized = False


def init(
    service_name: str = "pipecat-app",
    endpoint: str = "http://localhost:4317",
    insecure: bool = True,
) -> None:
    global _initialized

    if _initialized:
        logger.warning("finchvox.init() already called, skipping")
        return

    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from pipecat.utils.tracing.setup import setup_tracing

    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=insecure)
    setup_tracing(service_name=service_name, exporter=exporter)
    _initialized = True

    logger.info(f"finchvox initialized with service_name='{service_name}', endpoint='{endpoint}'")


from finchvox.processor import FinchvoxProcessor

__all__ = ["init", "FinchvoxProcessor"]
