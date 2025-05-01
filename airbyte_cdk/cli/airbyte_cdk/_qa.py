"""CLI command for running QA checks on connectors using pytest."""

import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

import rich_click as click

from airbyte_cdk.cli.airbyte_cdk._util import resolve_connector_name_and_directory


@click.command(name="pre-release-check")
@click.option(
    "-c",
    "--check",
    "selected_checks",
    multiple=True,
    help="The name of the check to run. If not provided, all checks will be run.",
)
@click.option(
    "--connector-name",
    type=str,
    help="Name of the connector to check. Ignored if --connector-directory is provided.",
)
@click.option(
    "--connector-directory",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Path to the connector directory.",
)
@click.option(
    "-r",
    "--report-path",
    "report_path",
    type=click.Path(file_okay=True, path_type=Path, writable=True, dir_okay=False),
    help="The path to the report file to write the results to as JSON.",
)
def pre_release_check(
    selected_checks: List[str],
    connector_name: Optional[str] = None,
    connector_directory: Optional[Path] = None,
    report_path: Optional[Path] = None,
) -> None:
    """Run pre-release checks on a connector using pytest.

    This command runs quality assurance checks on a connector to ensure it meets
    Airbyte's standards for release. The checks include:

    - Documentation checks
    - Metadata checks
    - Packaging checks
    - Security checks
    - Asset checks
    - Testing checks

    If no connector name or directory is provided, we will look within the current working
    directory. If the current working directory is not a connector directory (e.g. starting
    with 'source-') and no connector name or path is provided, the process will fail.
    """
    connector_name, connector_directory = resolve_connector_name_and_directory(
        connector_name=connector_name,
        connector_directory=connector_directory,
    )

    pytest_args = ["-xvs"]
    
    if connector_name:
        pytest_args.extend(["--connector-name", connector_name])
    if connector_directory:
        pytest_args.extend(["--connector-directory", str(connector_directory)])
    
    if report_path:
        pytest_args.extend(["--report-path", str(report_path)])
    
    if selected_checks:
        for check in selected_checks:
            pytest_args.extend(["-k", check])
    
    qa_module_path = Path(__file__).parent.parent.parent / "qa"
    pytest_args.extend(["-p", "airbyte_cdk.qa.pytest_plugin"])
    
    test_paths = []
    for root, _, files in os.walk(qa_module_path / "checks"):
        for file in files:
            if file.endswith("_test.py"):
                test_paths.append(os.path.join(root, file))
    
    cmd = [sys.executable, "-m", "pytest"] + pytest_args + test_paths
    click.echo(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    
    if result.returncode != 0:
        raise click.ClickException(f"Pytest failed with exit code {result.returncode}")
