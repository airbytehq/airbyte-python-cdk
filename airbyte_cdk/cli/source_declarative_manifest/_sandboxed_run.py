"""Use Firejail (if installed) to run the source in a sandboxed environment."""

import os
import subprocess
import sys

ENV_SANDBOX_MODE = "AIRBYTE_CONNECTOR_SANDBOX_MODE"
ENV_SANDBOX_MODE_FIREJAIL = "FIREJAIL"
ENV_SANDBOX_MODE_AUTO = "AUTO"  # Use Firejail if available, otherwise None
ENV_SANDBOX_MODE_NONE = "NONE"

USAGE = f"""
-------------------------------------------
-- source-declarative-manifest-sandboxed --
-------------------------------------------

Sandboxed execution of the source-declarative-manifest connector. By default, this script
wraps the source-declarative-manifest command in Firejail to run the connector in a sandboxed
environment. If Firejail is not available, the connector will run without sandboxing.

Environment variable '{ENV_SANDBOX_MODE}' controls the sandboxing behavior. The following values
are supported:
    - '{ENV_SANDBOX_MODE_FIREJAIL}': Use Firejail to run the connector in a sandboxed environment.
    - '{ENV_SANDBOX_MODE_AUTO}': Use Firejail if available, otherwise run without sandboxing.
    - '{ENV_SANDBOX_MODE_NONE}': Disable sandboxing and run the connector without Firejail.

Usage: source-declarative-manifest-sandboxed [OPTIONS] [CMD]

CMD:
  The command to run in the sandboxed environment. This should be the command
  that would normally be run to start the connector. E.g. "check", "read", etc.
  The command is passed to the source-declarative-manifest entrypoint.

  The command is ignored if specifying any of the below options.

Options:
  --help           Show this help message and exit.
  --check-sandbox  Check Firejail availability and exit.
"""


def _wrap_in_sandbox(cmd: list[str]) -> list[str]:
    """Wrap the given command in Firejail.
    This function modifies the command to include Firejail options
    and returns the updated command list.
    """
    sb_mode = os.getenv(ENV_SANDBOX_MODE, ENV_SANDBOX_MODE_AUTO).upper()
    if sb_mode == ENV_SANDBOX_MODE_NONE:
        print(
            f"WARNING: Sandboxing disabled because env var '{ENV_SANDBOX_MODE}={sb_mode}'.  "
            "Running without Firejail."
        )
        return cmd

    # Try Firejail
    try:
        subprocess.run(["firejail", "--version"], check=True, stdout=subprocess.PIPE)
    except FileNotFoundError:
        if sb_mode == ENV_SANDBOX_MODE_FIREJAIL:
            raise RuntimeError(
                f"Firejail required by env var value `{ENV_SANDBOX_MODE}={sb_mode}` but not found. "
                f"Set {ENV_SANDBOX_MODE} to 'NONE' to disable sandboxing or 'AUTO' to "
                "only use Firejail when available."
            )
        print("Firejail not found. Running without sandboxing.")
        return cmd

    # Firejail is available
    print("Firejail found. Running with Firejail sandboxing.")
    return ["firejail", "--private", "--net=none"] + cmd


def sandboxed_run():
    if len(sys.argv) == 1:
        _print_help()
        return

    if sys.argv[1] == "--help":
        _print_help()
        return

    if sys.argv[1] == "--check-sandbox":
        _print_sandbox_check()
        return

    executable = "source-declarative-manifest"  # Assume base SDM entrypoint is installed and on PATH

    full_cmd = _wrap_in_sandbox([executable] + sys.argv[1:])  # Preserve CLI arguments and wrap in Firejail
    print(f"Running command: {' '.join(full_cmd)}")

    # Execute the wrapped SDM command
    os.execvp(full_cmd[0], full_cmd)


def _print_help() -> None:
    print(USAGE)


def _print_sandbox_check() -> None:
    try:
        subprocess.run(["firejail", "--version"], check=True, stdout=subprocess.PIPE)
        print("Firejail found.")
    except FileNotFoundError:
        print("Firejail not found.")


if __name__ == "__main__":
    sandboxed_run()
