"""
FinchVox CLI - Command-line interface for the finchvox package.

Provides subcommands:
- finchvox start: Start the unified server
- finchvox version: Display version information
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

from finchvox.server import UnifiedServer
from finchvox.collector.config import GRPC_PORT
from finchvox import telemetry
from finchvox.storage.backend import StorageBackend


def get_version() -> str:
    """Get the package version."""
    # Read version from pyproject.toml or package metadata
    try:
        from importlib.metadata import version

        return version("finchvox")
    except Exception:
        return "0.0.1"  # Fallback version


def cmd_version(args):
    """Handle the 'version' subcommand."""
    print(f"finchvox version {get_version()}")
    print(f"Python {sys.version}")


def _create_storage_backend(args) -> Optional[StorageBackend]:
    storage_type = getattr(args, "storage", None) or os.environ.get(
        "FINCHVOX_STORAGE", "local"
    )

    if storage_type == "local":
        return None

    if storage_type == "s3":
        from finchvox.storage.s3 import S3Storage

        bucket = getattr(args, "s3_bucket", None) or os.environ.get(
            "FINCHVOX_S3_BUCKET"
        )
        if not bucket:
            print("Error: --s3-bucket or FINCHVOX_S3_BUCKET is required for S3 storage")
            sys.exit(1)

        region = getattr(args, "s3_region", None) or os.environ.get(
            "FINCHVOX_S3_REGION", "us-east-1"
        )
        prefix = getattr(args, "s3_prefix", None) or os.environ.get(
            "FINCHVOX_S3_PREFIX", "sessions"
        )
        endpoint = getattr(args, "s3_endpoint", None) or os.environ.get(
            "FINCHVOX_S3_ENDPOINT"
        )

        return S3Storage(
            bucket=bucket,
            region=region,
            prefix=prefix,
            endpoint_url=endpoint,
        )

    print(f"Error: Unknown storage type: {storage_type}")
    sys.exit(1)


def cmd_start(args):
    """Handle the 'start' subcommand."""
    if args.telemetry.lower() == "false":
        os.environ["FINCHVOX_TELEMETRY"] = "false"

    if args.data_dir:
        data_dir = Path(args.data_dir).expanduser().resolve()
    else:
        data_dir = Path.home() / ".finchvox"

    storage_backend = _create_storage_backend(args)

    print("=" * 50)
    print(f"Finchvox v{get_version()}")
    print(f"HTTP Server:  http://{args.host}:{args.port}")
    print(f"  - UI:       http://{args.host}:{args.port}")
    print(f"  - Collector: http://{args.host}:{args.port}/collector")
    print(f"gRPC Server:  {args.host}:{args.grpc_port}")
    print(f"Data Directory: {data_dir}")
    if storage_backend is not None:
        from finchvox.storage.s3 import S3Storage

        if isinstance(storage_backend, S3Storage):
            print(
                f"Storage: S3 (s3://{storage_backend.bucket}/{storage_backend.prefix})"
            )
    else:
        print("Storage: Local")
    if not telemetry.is_enabled():
        print("Telemetry: Disabled")
    print("=" * 50)

    server = UnifiedServer(
        port=args.port,
        grpc_port=args.grpc_port,
        host=args.host,
        data_dir=data_dir,
        storage_backend=storage_backend,
    )
    server.run()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="finchvox",
        description="FinchVox - Voice AI observability dev tool for Pipecat",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(
        title="commands",
        description="Available commands",
        dest="command",
        required=True,
    )

    # 'start' subcommand
    start_parser = subparsers.add_parser(
        "start",
        help="Start the unified server",
        description="Start the FinchVox unified server (gRPC + HTTP)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  finchvox start                           # Start with defaults (local storage)
  finchvox start --port 8000               # Custom HTTP port
  finchvox start --grpc-port 4318          # Custom gRPC port
  finchvox start --data-dir ./my-data      # Custom data directory
  finchvox start --storage=s3 --s3-bucket=mybucket  # Use S3 storage
  finchvox start --storage=s3 --s3-bucket=mybucket --s3-region=eu-west-1
  finchvox start --storage=s3 --s3-bucket=test --s3-endpoint=http://localhost:4566  # LocalStack
        """,
    )
    start_parser.add_argument(
        "--port", type=int, default=3000, help="HTTP server port (default: 3000)"
    )
    start_parser.add_argument(
        "--grpc-port",
        type=int,
        default=GRPC_PORT,
        help=f"gRPC server port (default: {GRPC_PORT})",
    )
    start_parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)"
    )
    start_parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Data directory for traces/logs/audio/exceptions (default: ~/.finchvox)",
    )
    start_parser.add_argument(
        "--telemetry",
        type=str,
        default="true",
        help="Enable or disable anonymous usage telemetry (default: true)",
    )
    start_parser.add_argument(
        "--storage",
        type=str,
        default="local",
        choices=["local", "s3"],
        help="Storage backend type (default: local)",
    )
    start_parser.add_argument(
        "--s3-bucket",
        type=str,
        default=None,
        help="S3 bucket name (required when --storage=s3)",
    )
    start_parser.add_argument(
        "--s3-region",
        type=str,
        default="us-east-1",
        help="S3 region (default: us-east-1)",
    )
    start_parser.add_argument(
        "--s3-prefix",
        type=str,
        default="sessions",
        help="S3 key prefix for sessions (default: sessions)",
    )
    start_parser.add_argument(
        "--s3-endpoint",
        type=str,
        default=None,
        help="S3 endpoint URL (for S3-compatible services like LocalStack)",
    )
    start_parser.set_defaults(func=cmd_start)

    # 'version' subcommand
    version_parser = subparsers.add_parser(
        "version",
        help="Display version information",
        description="Display FinchVox version and Python version",
    )
    version_parser.set_defaults(func=cmd_version)

    # Parse arguments and dispatch to handler
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
