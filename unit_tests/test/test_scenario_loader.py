# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
"""Unit tests for `airbyte_cdk.test.standard_tests.scenario_loader`."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import pytest

from airbyte_cdk.test.standard_tests.scenario_loader import (
    SigilResolutionError,
    load_metadata_smoke_test_scenarios,
)

_RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T00:00:00Z$")


@pytest.fixture
def connector_root(tmp_path: Path) -> Path:
    """Return a temp directory laid out like a connector root."""
    (tmp_path / "secrets").mkdir()
    return tmp_path


def _write_metadata(connector_root: Path, body: str) -> Path:
    metadata_path = connector_root / "metadata.yaml"
    metadata_path.write_text(dedent(body))
    return metadata_path


def _write_env(connector_root: Path, section: str, contents: str) -> None:
    (connector_root / "secrets" / f".env.{section}").write_text(dedent(contents))


def test_no_metadata_file_returns_empty(tmp_path: Path) -> None:
    """A missing metadata.yaml is not an error; scenario discovery is opt-in."""
    scenarios = load_metadata_smoke_test_scenarios(
        metadata_path=tmp_path / "metadata.yaml",
        connector_root=tmp_path,
    )
    assert scenarios == []


def test_metadata_without_smoke_test_suite_returns_empty(connector_root: Path) -> None:
    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: acceptanceTests
        """,
    )
    assert (
        load_metadata_smoke_test_scenarios(
            metadata_path=metadata_path,
            connector_root=connector_root,
        )
        == []
    )


def test_literal_only_scenario_resolves_without_provider_env(
    connector_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """source-faker-style scenarios resolve without any provider env vars."""
    monkeypatch.delenv("AIRBYTE_TEST_CREDS_PROVIDER", raising=False)
    monkeypatch.delenv("AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME", raising=False)

    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    count: 100
                    seed: "42"
        """,
    )

    scenarios = load_metadata_smoke_test_scenarios(
        metadata_path=metadata_path,
        connector_root=connector_root,
    )

    assert len(scenarios) == 1
    assert scenarios[0].id == "smoke-default"
    assert scenarios[0].config_dict == {"count": 100, "seed": "42"}


def test_relative_date_renders_rfc3339_at_midnight_utc(connector_root: Path) -> None:
    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    start_date: "${relative-date:-P30D}"
        """,
    )

    scenarios = load_metadata_smoke_test_scenarios(
        metadata_path=metadata_path,
        connector_root=connector_root,
    )

    assert len(scenarios) == 1
    rendered = scenarios[0].config_dict["start_date"]
    assert _RFC3339.match(rendered), rendered

    today = datetime.now(tz=timezone.utc)
    parsed = datetime.strptime(rendered, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    delta = today.replace(hour=0, minute=0, second=0, microsecond=0) - parsed
    assert delta.days == 30


def test_secret_ref_resolves_from_per_section_env_file(
    connector_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_PROVIDER", "1pass")
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME", "test-vault")

    _write_env(
        connector_root,
        "test-credentials-api_key",
        """
        ASHBY_API_KEY="super-secret"
        """,
    )

    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    api_key: "${secret-ref:ashby/test-credentials-api_key/api_key}"
                    start_date: "${relative-date:-P30D}"
        """,
    )

    scenarios = load_metadata_smoke_test_scenarios(
        metadata_path=metadata_path,
        connector_root=connector_root,
    )

    assert scenarios[0].config_dict["api_key"] == "super-secret"


def test_secret_ref_uses_literal_section_name(
    connector_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Literal section names with dashes/underscores must not be transformed."""
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_PROVIDER", "1pass")
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME", "test-vault")

    _write_env(
        connector_root,
        "test-credentials-oauth",
        """
        ASHBY_CLIENT_ID="client-abc"
        ASHBY_CLIENT_SECRET=client-secret-xyz
        """,
    )

    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: oauth
                  configSettings:
                    client_id: "${secret-ref:ashby/test-credentials-oauth/client_id}"
                    client_secret: "${secret-ref:ashby/test-credentials-oauth/client_secret}"
        """,
    )

    scenarios = load_metadata_smoke_test_scenarios(
        metadata_path=metadata_path,
        connector_root=connector_root,
    )

    assert scenarios[0].config_dict == {
        "client_id": "client-abc",
        "client_secret": "client-secret-xyz",
    }


def test_secret_ref_without_provider_env_fails(
    connector_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AIRBYTE_TEST_CREDS_PROVIDER", raising=False)
    monkeypatch.delenv("AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME", raising=False)

    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    api_key: "${secret-ref:ashby/test-credentials-api_key/api_key}"
        """,
    )

    with pytest.raises(SigilResolutionError, match="AIRBYTE_TEST_CREDS_PROVIDER"):
        load_metadata_smoke_test_scenarios(
            metadata_path=metadata_path,
            connector_root=connector_root,
        )


def test_secret_ref_without_vault_name_fails(
    connector_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_PROVIDER", "1pass")
    monkeypatch.delenv("AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME", raising=False)

    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    api_key: "${secret-ref:ashby/test-credentials-api_key/api_key}"
        """,
    )

    with pytest.raises(SigilResolutionError, match="VAULT_NAME"):
        load_metadata_smoke_test_scenarios(
            metadata_path=metadata_path,
            connector_root=connector_root,
        )


def test_secret_ref_missing_env_file_fails(
    connector_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_PROVIDER", "1pass")
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME", "test-vault")

    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    api_key: "${secret-ref:ashby/test-credentials-api_key/api_key}"
        """,
    )

    with pytest.raises(SigilResolutionError, match=r"\.env\.test-credentials-api_key"):
        load_metadata_smoke_test_scenarios(
            metadata_path=metadata_path,
            connector_root=connector_root,
        )


def test_secret_ref_missing_env_var_in_file_fails(
    connector_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_PROVIDER", "1pass")
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME", "test-vault")

    _write_env(
        connector_root,
        "test-credentials-api_key",
        """
        ASHBY_OTHER=irrelevant
        """,
    )

    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    api_key: "${secret-ref:ashby/test-credentials-api_key/api_key}"
        """,
    )

    with pytest.raises(SigilResolutionError, match="ASHBY_API_KEY"):
        load_metadata_smoke_test_scenarios(
            metadata_path=metadata_path,
            connector_root=connector_root,
        )


def test_unknown_sigil_fails(connector_root: Path) -> None:
    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    weird: "${unknown-sigil:foo}"
        """,
    )

    with pytest.raises(SigilResolutionError, match="Unrecognized smoke-test sigil"):
        load_metadata_smoke_test_scenarios(
            metadata_path=metadata_path,
            connector_root=connector_root,
        )


def test_two_segment_secret_ref_is_not_supported(connector_root: Path) -> None:
    """3-segment-only is locked: 2-segment must fail loudly."""
    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    api_key: "${secret-ref:ashby/api_key}"
        """,
    )

    with pytest.raises(SigilResolutionError, match="Unrecognized smoke-test sigil"):
        load_metadata_smoke_test_scenarios(
            metadata_path=metadata_path,
            connector_root=connector_root,
        )


def test_multiple_scenarios_resolve_independently(
    connector_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_PROVIDER", "1pass")
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME", "test-vault")

    _write_env(connector_root, "test-credentials-api_key", 'ASHBY_API_KEY="k1"')
    _write_env(
        connector_root,
        "test-credentials-oauth",
        'ASHBY_CLIENT_ID="cid"\nASHBY_CLIENT_SECRET="csec"',
    )

    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    api_key: "${secret-ref:ashby/test-credentials-api_key/api_key}"
                - name: oauth
                  configSettings:
                    client_id: "${secret-ref:ashby/test-credentials-oauth/client_id}"
                    client_secret: "${secret-ref:ashby/test-credentials-oauth/client_secret}"
        """,
    )

    scenarios = load_metadata_smoke_test_scenarios(
        metadata_path=metadata_path,
        connector_root=connector_root,
    )

    assert [s.id for s in scenarios] == ["smoke-default", "smoke-oauth"]
    assert scenarios[0].config_dict == {"api_key": "k1"}
    assert scenarios[1].config_dict == {"client_id": "cid", "client_secret": "csec"}


def test_nested_structures_are_resolved_recursively(
    connector_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_PROVIDER", "1pass")
    monkeypatch.setenv("AIRBYTE_TEST_CREDS_1PASS_VAULT_NAME", "test-vault")
    _write_env(connector_root, "test-credentials-api_key", 'ASHBY_API_KEY="k1"')

    metadata_path = _write_metadata(
        connector_root,
        """
        data:
          connectorTestSuitesOptions:
            - suite: smokeTests
              scenarios:
                - name: default
                  configSettings:
                    credentials:
                      auth_type: api_key
                      api_key: "${secret-ref:ashby/test-credentials-api_key/api_key}"
                    streams:
                      - name: candidates
                        cursor: "${relative-date:-P30D}"
        """,
    )

    scenarios = load_metadata_smoke_test_scenarios(
        metadata_path=metadata_path,
        connector_root=connector_root,
    )

    config = scenarios[0].config_dict
    assert config["credentials"] == {"auth_type": "api_key", "api_key": "k1"}
    assert _RFC3339.match(config["streams"][0]["cursor"])
