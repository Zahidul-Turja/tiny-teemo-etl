"""
CLI entrypoints for uv run / pip-installed scripts.

  uv run dev    → development server with auto-reload
  uv run start  → production server (4 workers)
  uv run test   → pytest test suite
"""

import subprocess
import sys


def dev():
    """Run the development server with auto-reload."""
    sys.exit(
        subprocess.call(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "main:app",
                "--reload",
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
            ]
        )
    )


def start():
    """Run the production server with 4 workers."""
    sys.exit(
        subprocess.call(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "main:app",
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
                "--workers",
                "4",
            ]
        )
    )


def test():
    """Run the pytest test suite."""
    sys.exit(
        subprocess.call(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/",
                "-v",
                *sys.argv[1:],  # forward any extra args, e.g. uv run test -k my_test
            ]
        )
    )
