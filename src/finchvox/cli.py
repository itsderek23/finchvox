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
from finchvox.server import UnifiedServer
from finchvox.collector.config import GRPC_PORT


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


def cmd_start(args):
    """Handle the 'start' subcommand."""
    if args.telemetry.lower() == "false":
        os.environ["FINCHVOX_TELEMETRY"] = "false"

    if args.data_dir:
        data_dir = Path(args.data_dir).expanduser().resolve()
    else:
        data_dir = Path.home() / ".finchvox"

    print("Starting FinchVox Unified Server...")
    print("=" * 50)
    print(f"HTTP Server:  http://{args.host}:{args.port}")
    print(f"  - UI:       http://{args.host}:{args.port}")
    print(f"  - Collector: http://{args.host}:{args.port}/collector")
    print(f"gRPC Server:  {args.host}:{args.grpc_port}")
    print(f"Data Directory: {data_dir}")
    print("=" * 50)

    server = UnifiedServer(
        port=args.port,
        grpc_port=args.grpc_port,
        host=args.host,
        data_dir=data_dir
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
        required=True
    )

    # 'start' subcommand
    start_parser = subparsers.add_parser(
        "start",
        help="Start the unified server",
        description="Start the FinchVox unified server (gRPC + HTTP)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  finchvox start                           # Start with defaults
  finchvox start --port 8000               # Custom HTTP port
  finchvox start --grpc-port 4318          # Custom gRPC port
  finchvox start --data-dir ./my-data      # Custom data directory
        """
    )
    start_parser.add_argument(
        "--port",
        type=int,
        default=3000,
        help="HTTP server port (default: 3000)"
    )
    start_parser.add_argument(
        "--grpc-port",
        type=int,
        default=GRPC_PORT,
        help=f"gRPC server port (default: {GRPC_PORT})"
    )
    start_parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)"
    )
    start_parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Data directory for traces/logs/audio/exceptions (default: ~/.finchvox)"
    )
    start_parser.add_argument(
        "--telemetry",
        type=str,
        default="true",
        help="Enable or disable anonymous usage telemetry (default: true)"
    )
    start_parser.set_defaults(func=cmd_start)

    # 'version' subcommand
    version_parser = subparsers.add_parser(
        "version",
        help="Display version information",
        description="Display FinchVox version and Python version"
    )
    version_parser.set_defaults(func=cmd_version)

    # Parse arguments and dispatch to handler
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
