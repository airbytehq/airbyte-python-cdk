from pathlib import Path
from tabnanny import check

import pytest

from airbyte_cdk.utils.connector_paths import resolve_airbyte_repo_root

CDK_REPO_ROOT = Path(__file__).resolve().parents[3].absolute()


def check_local_repo_structure() -> bool:
    """
    Check if the local repository structure is correct.
    This function checks if the current working directory is 'airbyte-cdk'.
    """
    return all(
        [
            CDK_REPO_ROOT.name == "airbyte-python-cdk",
            (CDK_REPO_ROOT.parent / "airbyte").is_dir(),
            (CDK_REPO_ROOT.parent / "airbyte-enterprise").is_dir(),
            (CDK_REPO_ROOT.parent / "airbyte" / "airbyte-integrations").is_dir(),
            (CDK_REPO_ROOT.parent / "airbyte-enterprise" / "airbyte-integrations").is_dir(),
        ]
    )


@pytest.mark.parametrize(
    "start_dir_rel, expect_success",
    [
        (CDK_REPO_ROOT / ".." / "airbyte", True),
        (CDK_REPO_ROOT / ".." / "airbyte" / "airbyte-ci", True),
        (CDK_REPO_ROOT.parent, True),  # Parent directory from CDK repo
        (CDK_REPO_ROOT.parent.parent, False),  # Grandparent directory from CDK repo
        (CDK_REPO_ROOT / ".." / "airbyte-enterprise", True),
        (CDK_REPO_ROOT / ".." / "airbyte-enterprise" / "airbyte-integrations", True),
        (Path("/"), False),  # Filesystem root
        (Path("/unrelated"), False),
        (Path("/unrelated/foo"), False),
    ],
)
@pytest.mark.skipif(
    not check_local_repo_structure(),
    reason=(
        "Test requires a specific local repository structure with "
        "'airbyte' and 'airbyte-enterprise' checked out."
    ),
)
def test_resolve_airbyte_repo_root_real_fs(
    start_dir_rel: Path,
    expect_success: bool,
):
    """
    This test assumes that the developer's workstation has the following sibling directories checked out:
      - airbyte
      - airbyte-cdk
      - airbyte-enterprise
    in the same parent directory (e.g., ~/repos/).

    The test will skip a scenario if the required directory does not exist.
    """
    try:
        repo_root = resolve_airbyte_repo_root(start_dir_rel)
        if repo_root is None:
            raise AssertionError(
                f"Airbyte repo root should not be None, from: '{start_dir_rel!s}'."
            )
        if repo_root.name != "airbyte" and repo_root.name != "airbyte-enterprise":
            raise AssertionError(
                f"Airbyte repo root should be 'airbyte' or 'airbyte-enterprise', "
                f"but found: '{repo_root.name!s}'"
            )
        if not repo_root.is_dir():
            raise AssertionError(
                f"Found Airbyte repo root, but it is not a directory: {repo_root!s}"
            )
        if not expect_success:
            raise AssertionError(
                f"Airbyte repo root found from '{start_dir_rel!s}' when it was not expected."
                f"Found: {repo_root!s}"
            )
    except FileNotFoundError:
        if expect_success:
            raise AssertionError(f"Airbyte repo root not found from '{start_dir_rel!s}'.")
