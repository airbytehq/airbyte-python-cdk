"""Use Firejail (if installed) to run the source in a sandboxed environment."""

import os
import subprocess
import sys

ENV_SANDBOX_MODE = "AIRBYTE_CONNECTOR_SANDBOX_MODE"
ENV_SANDBOX_MODE_FIREJAIL = "FIREJAIL"
ENV_SANDBOX_MODE_AUTO = "AUTO"  # Use Firejail if available, otherwise None
ENV_SANDBOX_MODE_NONE = "NONE"


def _wrap_in_sandbox(cmd: list[str]) -> list[str]:
    """Wrap the given command in Firejail.
    This function modifies the command to include Firejail options
    and returns the updated command list.
    """
    sb_mode = os.getenv(ENV_SANDBOX_MODE, ENV_SANDBOX_MODE_AUTO).upper()
    if sb_mode == ENV_SANDBOX_MODE_NONE:
        print(
            f"WARNING: Sandboxing disabled due to env variable '{ENV_SANDBOX_MODE}'.  "
            "Running without Firejail."
        )
        return cmd

    # Try Firejail
    try:
        print("Checking for Firejail...")
        subprocess.run(["firejail", "--version"], check=True, stdout=subprocess.PIPE)
    except FileNotFoundError:
        if sb_mode == ENV_SANDBOX_MODE_FIREJAIL:
            raise RuntimeError(
                "Firejail not found. Set AIRBYTE_CONNECTOR_SANDBOX_MODE=NONE to disable sandboxing."
            )
        print("Firejail not found. Running without sandboxing.")
        return cmd

    # Firejail is available
    print("Firejail found. Running with Firejail sandboxing.")
    return ["firejail", "--private", "--net=none"] + cmd


def sandboxed_run():
    executable = "source-declarative-manifest"  # Assume base SDM entrypoint is installed and on PATH

    full_cmd = _wrap_in_sandbox([executable] + sys.argv[1:])  # Preserve CLI arguments and wrap in Firejail
    print(f"Running command: {' '.join(full_cmd)}")

    # Execute the wrapped SDM command
    os.execvp(full_cmd[0], full_cmd)


if __name__ == "__main__":
    sandboxed_run()
