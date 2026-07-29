# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Tests for `build_connector_image` in `airbyte_cdk.utils.docker`."""

import shutil
from pathlib import Path

import pytest

from airbyte_cdk.models.connector_metadata import MetadataFile
from airbyte_cdk.utils import docker

POKEAPI_FIXTURE_DIR = Path(__file__).parent.parent / "resources" / "source_pokeapi_w_components_py"


@pytest.mark.parametrize(
    "base_image_override, expected_base_image",
    [
        pytest.param(
            None,
            "docker.io/airbyte/source-declarative-manifest:6.51.0@sha256:890b109f243b8b9406f23ea7522de41025f7b3e87f6fc9710bc1e521213a276f",
            id="default_uses_metadata_base_image",
        ),
        pytest.param(
            "airbyte/source-declarative-manifest:dev",
            "airbyte/source-declarative-manifest:dev",
            id="override_replaces_metadata_base_image",
        ),
    ],
)
def test_build_connector_image_base_image_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    base_image_override: str | None,
    expected_base_image: str,
) -> None:
    # Copy the fixture so the Dockerfile templates that `build_connector_image` writes into
    # `<connector>/build/docker/` land in a temp dir instead of the checked-in fixture, and stub
    # template resolution, which otherwise falls back to downloading templates from GitHub when
    # no airbyte monorepo checkout is present.
    connector_directory = tmp_path / POKEAPI_FIXTURE_DIR.name
    shutil.copytree(POKEAPI_FIXTURE_DIR, connector_directory)
    monkeypatch.setattr(
        docker,
        "get_dockerfile_templates",
        lambda *, metadata, connector_directory: ("FROM ${BASE_IMAGE}\n", ""),
    )
    metadata = MetadataFile.from_file(connector_directory / "metadata.yaml")

    captured_build_args: list[dict[str, str | None]] = []

    def fake_build_image(
        *,
        context_dir: Path,
        dockerfile: Path,
        metadata: MetadataFile,
        tag: str,
        arch: docker.ArchEnum,
        build_args: dict[str, str | None],
    ) -> str:
        captured_build_args.append(build_args)
        return tag

    monkeypatch.setattr(docker, "_build_image", fake_build_image)
    monkeypatch.setattr(docker, "_tag_image", lambda *, tag, new_tags: None)

    docker.build_connector_image(
        connector_name="source-pokeapi",
        connector_directory=connector_directory,
        metadata=metadata,
        tag="test-tag",
        no_verify=True,
        base_image_override=base_image_override,
    )

    assert captured_build_args, "Expected at least one image build."
    for build_args in captured_build_args:
        assert build_args["BASE_IMAGE"] == expected_base_image
