from loguru import logger
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import TraceServiceServicer
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceResponse,
    ExportTracePartialSuccess,
)
from .writer import SpanWriter


class TraceCollectorServicer(TraceServiceServicer):
    """Implements the OTLP TraceService gRPC interface."""

    def __init__(self, span_writer: SpanWriter):
        self.span_writer = span_writer

    def Export(self, request, context):
        """Handle incoming trace export requests."""
        try:
            span_count = 0
            span_names = []
            for resource_spans in request.resource_spans:
                for scope_spans in resource_spans.scope_spans:
                    for span in scope_spans.spans:
                        self.span_writer.write_span(span, resource_spans, scope_spans)
                        span_names.append(span.name)
                        span_count += 1

            logger.info(f"Successfully processed {span_count} spans={span_names}")
            return ExportTraceServiceResponse(
                partial_success=ExportTracePartialSuccess(
                    rejected_spans=0,
                    error_message=""
                )
            )
        except Exception as e:
            logger.error(f"Error processing spans: {e}", exc_info=True)
            # Continue processing - return partial success
            return ExportTraceServiceResponse(
                partial_success=ExportTracePartialSuccess(
                    rejected_spans=0,  # Could track actual failures
                    error_message=str(e)
                )
            )
