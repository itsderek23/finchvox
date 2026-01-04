"""
FinchVox entry point - starts unified server with collector and UI.

Usage:
    python -m finchvox                    # Start with default port 3000
    python -m finchvox --port 8000        # Start with custom port
    python -m finchvox --help             # Show options
"""

import argparse
from pathlib import Path
from finchvox.server import UnifiedServer
from finchvox.collector.config import GRPC_PORT, get_default_data_dir


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="FinchVox unified server for voice AI observability",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m finchvox                           # Start with default ports
  python -m finchvox --port 8000               # Use custom HTTP port
  python -m finchvox --grpc-port 4318          # Use custom gRPC port
  python -m finchvox --data-dir ./my-data      # Use custom data directory
        """
    )
    parser.add_argument(
        "--port",
        type=int,
        default=3000,
        help="HTTP server port (default: 3000)"
    )
    parser.add_argument(
        "--grpc-port",
        type=int,
        default=GRPC_PORT,
        help=f"gRPC server port (default: {GRPC_PORT})"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Data directory for traces/logs/audio/exceptions (default: ~/.finchvox)"
    )

    args = parser.parse_args()

    # Resolve data directory
    if args.data_dir:
        data_dir = Path(args.data_dir).expanduser().resolve()
    else:
        data_dir = get_default_data_dir()

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


if __name__ == "__main__":
    main()
