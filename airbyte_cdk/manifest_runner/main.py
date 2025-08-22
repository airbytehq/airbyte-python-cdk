# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Main entry point for the Airbyte CDK Manifest Runner server."""

import sys

import uvicorn


def check_dependencies() -> bool:
    """Check if all required dependencies are available."""
    try:
        import fastapi
        import uvicorn

        return True
    except ImportError:
        return False


def run_server(
    host: str = "127.0.0.1", port: int = 8000, reload: bool = False, log_level: str = "info"
) -> None:
    """Run the FastAPI server."""
    if not check_dependencies():
        print("❌ Manifest runner dependencies not found. Please install with:")
        print("  pip install airbyte-cdk[manifest-runner]")
        print("  # or")
        print("  poetry install --extras manifest-runner")
        sys.exit(1)

    print(f"🚀 Starting Airbyte CDK Manifest Runner on {host}:{port}")

    uvicorn.run(
        "airbyte_cdk.manifest_runner.app:app",
        host=host,
        port=port,
        reload=reload,
        log_level=log_level,
    )


if __name__ == "__main__":
    run_server()
