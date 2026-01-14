from loguru import logger
from opentelemetry.proto.collector.logs.v1.logs_service_pb2_grpc import LogsServiceServicer
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import (
    ExportLogsServiceResponse,
    ExportLogsPartialSuccess,
)
from .log_writer import LogWriter


class LogCollectorServicer(LogsServiceServicer):
    """Implements the OTLP LogsService gRPC interface."""

    def __init__(self, log_writer: LogWriter):
        self.log_writer = log_writer

    def Export(self, request, context):
        """Handle incoming log export requests."""
        try:
            log_count = 0
            for resource_logs in request.resource_logs:
                for scope_logs in resource_logs.scope_logs:
                    for log_record in scope_logs.log_records:
                        self.log_writer.write_log(log_record, resource_logs, scope_logs)
                        log_count += 1

            logger.info(f"Processed {log_count} log records")
            return ExportLogsServiceResponse(
                partial_success=ExportLogsPartialSuccess(
                    rejected_log_records=0,
                    error_message=""
                )
            )
        except Exception as e:
            logger.error(f"Error processing logs: {e}", exc_info=True)
            return ExportLogsServiceResponse(
                partial_success=ExportLogsPartialSuccess(
                    rejected_log_records=0,
                    error_message=str(e)
                )
            )
