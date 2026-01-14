import logging
from loguru import logger

_initialized = False
_allowed_log_modules: set[str] = {"pipecat.", "finchvox.", "__main__"}


def init(
    service_name: str = "pipecat-app",
    endpoint: str = "http://localhost:4317",
    insecure: bool = True,
    capture_logs: bool = True,
    log_modules: list[str] | None = None,
) -> None:
    global _initialized, _allowed_log_modules

    if _initialized:
        logger.warning("finchvox.init() already called, skipping")
        return

    if log_modules:
        for mod in log_modules:
            _allowed_log_modules.add(mod if mod.endswith(".") else f"{mod}.")

    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from pipecat.utils.tracing.setup import setup_tracing

    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=insecure)
    setup_tracing(service_name=service_name, exporter=exporter)

    if capture_logs:
        _setup_log_capture(service_name, endpoint, insecure)

    _initialized = True
    logger.info(f"finchvox initialized with service_name='{service_name}', endpoint='{endpoint}', capture_logs={capture_logs}")


class TraceContextLoggingHandler(logging.Handler):
    """LoggingHandler that only emits logs with active Pipecat trace context.

    This handler wraps OpenTelemetry's LoggingHandler and only forwards logs
    when there's an active trace context from Pipecat's TurnContextProvider
    or ConversationContextProvider.

    The key difference from using a Filter is that this handler keeps the
    trace context attached during the entire emit() call, including when
    LoggingHandler._translate() calls get_current() to read the trace context.

    Logs without trace context are silently dropped here but still appear
    in the console via other handlers (loguru's default sink).
    """

    def __init__(self, otel_handler: logging.Handler):
        super().__init__()
        self._otel_handler = otel_handler

    def emit(self, record: logging.LogRecord) -> None:
        module = record.name or ""
        if not self._is_allowed_module(module):
            return

        ctx = _get_pipecat_context()
        if ctx:
            from opentelemetry.context import attach, detach
            token = attach(ctx)
            try:
                self._otel_handler.emit(record)
            finally:
                detach(token)

    def _is_allowed_module(self, module: str) -> bool:
        for prefix in _allowed_log_modules:
            if prefix == "__main__":
                if module == "__main__":
                    return True
            elif module.startswith(prefix):
                return True
        return False


def _setup_log_capture(service_name: str, endpoint: str, insecure: bool) -> None:
    """Set up OpenTelemetry log capture and export."""
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.instrumentation.logging import LoggingInstrumentor

    resource = Resource.create({"service.name": service_name})
    logger_provider = LoggerProvider(resource=resource)

    log_exporter = OTLPLogExporter(endpoint=endpoint, insecure=insecure)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

    otel_handler = LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider)
    wrapper_handler = TraceContextLoggingHandler(otel_handler)
    logging.getLogger().addHandler(wrapper_handler)
    logging.getLogger().setLevel(logging.DEBUG)

    LoggingInstrumentor().instrument(set_logging_format=True)

    _setup_loguru_bridge()

    logger.debug("OpenTelemetry log capture initialized")


def _get_pipecat_context():
    """Get current trace context from Pipecat's turn or conversation provider.

    OpenTelemetry's LoggingInstrumentor reads trace context from thread-local
    contextvars, but Pipecat stores the current turn's span context in a custom
    singleton (TurnContextProvider). This function bridges the gap by retrieving
    the context from Pipecat's providers so we can inject it into thread-local
    storage before forwarding logs.
    """
    try:
        from pipecat.utils.tracing.turn_context_provider import get_current_turn_context
        ctx = get_current_turn_context()
        if ctx:
            return ctx
        from pipecat.utils.tracing.conversation_context_provider import get_current_conversation_context
        return get_current_conversation_context()
    except ImportError:
        return None


def _setup_loguru_bridge() -> None:
    """Bridge loguru logs to stdlib logging so they get captured by OTel.

    Uses logger.configure(patcher=...) instead of logger.add(sink) because
    patchers survive logger.remove() calls. This is important because Pipecat's
    runner calls logger.remove() to reconfigure logging.

    This bridge also injects trace context from Pipecat's TurnContextProvider
    into thread-local storage before forwarding logs, ensuring logs during
    active turns are associated with the correct trace.
    """
    from loguru import logger as loguru_logger

    def log_patcher(record):
        level_name = record["level"].name
        stdlib_level = getattr(logging, level_name.upper(), logging.INFO)
        module_name = record["name"] or "loguru"

        turn_context = _get_pipecat_context()

        if turn_context:
            from opentelemetry.context import attach, detach
            token = attach(turn_context)
            try:
                logging.getLogger(module_name).log(stdlib_level, record["message"])
            finally:
                detach(token)
        else:
            logging.getLogger(module_name).log(stdlib_level, record["message"])

    loguru_logger.configure(patcher=log_patcher)


from finchvox.processor import FinchvoxProcessor

__all__ = ["init", "FinchvoxProcessor"]
