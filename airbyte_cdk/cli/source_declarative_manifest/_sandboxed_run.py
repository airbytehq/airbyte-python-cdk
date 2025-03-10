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


def _get_default_gateway() -> str | None:
    """Returns the system's default gateway IP, or None if not found."""
    try:
        result = subprocess.run(
            ["ip", "route"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        for line in result.stdout.split("\n"):
            if line.startswith("default via"):
                return line.split()[2]  # Extract the gateway IP

    except Exception as e:
        print(f"❌ Error detecting gateway: {e}")

    return None


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
    gateway_ip: str | None = _get_default_gateway()
    if not gateway_ip:
        print("❌ No gateway detected. Blocking all egress traffic.")
        netfilter_rules = [
            "--netfilter=reject 0.0.0.0/0"  # Block all traffic if no gateway is detected
        ]
    else:
        print(f"🔥 Allowing egress only via gateway: {gateway_ip}")
        netfilter_rules = [
            # Allow traffic only via the detected gateway:
            f"--netfilter=accept {gateway_ip}",
            # Block all private networks and localhost:
            "--netfilter=reject 10.0.0.0/8",
            "--netfilter=reject 172.16.0.0/12",
            "--netfilter=reject 192.168.0.0/16",
            "--netfilter=reject 127.0.0.0/8",
            "--netfilter=reject 169.254.0.0/16",
            "--netfilter=reject ::1/128",
            "--netfilter=reject fc00::/7",
            "--netfilter=reject fe80::/10",
        ]

    # Use Google DNS resolution. It is fine to use others but we just don't want to block DNS.
    dns_args = [
        "--dns=8.8.8.8",
        "--dns=8.8.4.4",
    ]
    # Firejail Command with DNS resolution allowed
    firejail_args = (
        [
            "--private",  # Isolates the filesystem (prevents access to user files)
            "--net=none",  # Ensures restricted networking
            "--seccomp",  # Enable seccomp-bpf syscall filtering
            "--blacklist=/var/run/"  # Block system sockets
            "--private-tmp"  # Creates an isolated temp directory for each process
            "--rlimit-cpu=120",  # Limits CPU time to 1 second (prevents runaway processes)
            "--rlimit-fsize=10000000",  # Limits max file size to 10 MB (prevents excessive disk writes)
        ]
        + netfilter_rules
        + dns_args
    )

    print("Firejail found. Running with Firejail sandboxing.")
    return ["firejail"] + firejail_args + cmd


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
