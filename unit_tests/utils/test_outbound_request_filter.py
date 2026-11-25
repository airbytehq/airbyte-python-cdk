import ipaddress
import socket
from unittest.mock import Mock

import pytest
import requests_mock
from requests import PreparedRequest, Response, Session
from requests.exceptions import InvalidURL

from airbyte_cdk.utils.outbound_request_filter import (
    BlockKind,
    DestinationClass,
    OutboundRequestPolicy,
    _normalize_hostname,
    _reset_request_filter_policy_for_testing,
    _resolve_cname_chain,
    _validate_prepared_request,
    cloud_outbound_request_policy,
    init_internal_request_filter,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

_ORIGINAL_SESSION_SEND = Session.send


@pytest.fixture(autouse=True)
def reset_process_filter(monkeypatch):
    monkeypatch.setattr(Session, "send", _ORIGINAL_SESSION_SEND)
    _reset_request_filter_policy_for_testing()
    yield
    monkeypatch.setattr(Session, "send", _ORIGINAL_SESSION_SEND)
    _reset_request_filter_policy_for_testing()


def _request(url: str) -> PreparedRequest:
    request = PreparedRequest()
    request.method = "GET"
    request.headers = {}
    request.url = url
    return request


def _address_info(*addresses: str, canonical_name: str = ""):
    return [
        (
            socket.AF_INET6 if ":" in address else socket.AF_INET,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP,
            canonical_name if index == 0 else "",
            (address, 443, 0, 0) if ":" in address else (address, 443),
        )
        for index, address in enumerate(addresses)
    ]


@pytest.mark.parametrize(
    "host,address,kind,destination_class",
    [
        ("api.airbyte.com", None, "hostname", "protected_service"),
        ("denied.example", None, "hostname", "other_policy"),
        ("target.example", "fe80::1", "address", "metadata_or_link_local"),
        ("target.example", "192.0.2.1", "address", "private_or_non_global"),
        ("target.example", "8.8.8.8", "address", "restricted_network"),
    ],
)
def test_deny_observer_receives_only_classification_before_send(
    mocker, host, address, kind, destination_class
) -> None:
    send = mocker.patch.object(Session, "send", autospec=True)
    resolver = mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info(address) if address else [],
    )
    observed: list[tuple[BlockKind, DestinationClass]] = []

    def observe(block_kind: BlockKind, destination: DestinationClass) -> None:
        observed.append((block_kind, destination))

    init_internal_request_filter(
        OutboundRequestPolicy(
            blocked_hosts=frozenset({"api.airbyte.com", "denied.example"}),
            blocked_networks=(ipaddress.ip_network("8.8.8.0/24"),),
        ),
        deny_observer=observe,
    )

    with pytest.raises(AirbyteTracedException):
        Session().send(_request(f"https://{host}/private-synthetic-path"))

    assert observed == [(kind, destination_class)]
    send.assert_not_called()
    assert resolver.call_count == int(address is not None)


@pytest.mark.parametrize("observer_fails", [True, False])
def test_observer_failure_or_absence_preserves_deny(mocker, observer_fails) -> None:
    send = mocker.patch.object(Session, "send", autospec=True)
    observer = Mock(side_effect=RuntimeError) if observer_fails else None
    init_internal_request_filter(
        OutboundRequestPolicy(blocked_hosts=frozenset({"denied.example"})),
        deny_observer=observer,
    )

    with pytest.raises(AirbyteTracedException, match="Invalid URL endpoint"):
        Session().send(_request("https://denied.example/"))

    if observer is not None:
        observer.assert_called_once_with("hostname", "other_policy")
    send.assert_not_called()


def test_dns_failure_does_not_notify_policy_observer(mocker) -> None:
    resolver = mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        side_effect=socket.gaierror,
    )
    observer = Mock()
    init_internal_request_filter(deny_observer=observer)

    with pytest.raises(InvalidURL):
        Session().send(_request("https://target.example/"))

    resolver.assert_called_once()
    observer.assert_not_called()


@pytest.mark.parametrize(
    "address",
    [
        "10.0.0.1",
        "100.64.0.1",
        "127.0.0.1",
        "169.254.169.254",
        "192.0.2.1",
        "224.0.0.1",
        "::1",
        "::ffff:127.0.0.1",
        "fe80::1",
        "ff02::1",
    ],
)
def test_non_global_addresses_are_rejected(mocker, address):
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info(address),
    )

    with pytest.raises(AirbyteTracedException) as exc_info:
        _validate_prepared_request(_request("https://target.example/"))
    assert exc_info.value.internal_message
    assert address in exc_info.value.internal_message


def test_any_denied_address_rejects_a_mixed_ipv4_ipv6_response(mocker):
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info("8.8.8.8", "fd00::1"),
    )

    with pytest.raises(AirbyteTracedException) as exc_info:
        _validate_prepared_request(_request("https://target.example/"))
    assert exc_info.value.internal_message
    assert "fd00::1" in exc_info.value.internal_message


def test_unrelated_global_address_is_allowed(mocker):
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info("8.8.8.8", "2001:4860:4860::8888"),
    )

    _validate_prepared_request(_request("https://target.example/"))


@pytest.mark.parametrize(
    "hostname",
    [
        "MCP.AIRBYTE.COM.",
        "api.airbyte.com",
        "cloud.airbyte.io.",
        "CONNECTORS.AIRBYTE.COM",
    ],
)
def test_manifest_server_policy_rejects_normalized_airbyte_hostnames(hostname):
    policy = cloud_outbound_request_policy(include_airbyte_service_names=True)
    init_internal_request_filter(policy)

    with pytest.raises(AirbyteTracedException) as exc_info:
        Session().send(_request(f"https://{hostname}/"))
    assert exc_info.value.internal_message
    assert "hostname" in exc_info.value.internal_message


def test_configured_suffix_matching_uses_dns_label_boundaries(mocker, monkeypatch):
    underlying_send = mocker.patch.object(Session, "send", return_value=Response())
    monkeypatch.setenv("AIRBYTE_SSRF_DENY_HOST_SUFFIXES", "service.example")
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info("8.8.8.8"),
    )
    policy = cloud_outbound_request_policy()
    init_internal_request_filter(policy)

    response = Session().send(_request("https://service.example.attacker.example/"))

    assert isinstance(response, Response)
    underlying_send.assert_called_once()


@pytest.mark.parametrize("hostname", ["service.example", "api.service.example."])
def test_configured_suffix_rejects_apex_and_subdomains(monkeypatch, hostname):
    monkeypatch.setenv("AIRBYTE_SSRF_DENY_HOST_SUFFIXES", "service.example")
    init_internal_request_filter(cloud_outbound_request_policy())

    with pytest.raises(AirbyteTracedException) as exc_info:
        Session().send(_request(f"https://{hostname}/"))

    assert exc_info.value.internal_message
    assert "hostname" in exc_info.value.internal_message


def test_idna_hostnames_are_normalized_for_policy_matching(monkeypatch):
    monkeypatch.setenv("AIRBYTE_SSRF_DENY_HOSTS", "bücher.example")
    monkeypatch.setenv("AIRBYTE_SSRF_DENY_HOST_SUFFIXES", ".internal.example")
    policy = cloud_outbound_request_policy()
    assert "xn--bcher-kva.example" in policy.blocked_hosts
    assert "internal.example" in policy.blocked_suffixes
    assert _normalize_hostname("BÜCHER.EXAMPLE.") == "xn--bcher-kva.example"


@pytest.mark.parametrize(
    ("cidr", "address"),
    [
        ("35.1.2.3/32", "35.1.2.3"),
        ("2001:4860:4860::8888/128", "2001:4860:4860::8888"),
    ],
)
def test_configured_public_cidr_is_rejected(mocker, monkeypatch, cidr, address):
    monkeypatch.setenv("AIRBYTE_SSRF_DENY_CIDRS", cidr)
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info(address),
    )
    init_internal_request_filter(cloud_outbound_request_policy())

    with pytest.raises(AirbyteTracedException) as exc_info:
        Session().send(_request("https://public-lb.example/"))
    assert exc_info.value.internal_message
    assert address in exc_info.value.internal_message


def test_mapped_public_ipv6_is_checked_against_ipv4_deny_network(mocker, monkeypatch):
    monkeypatch.setenv("AIRBYTE_SSRF_DENY_CIDRS", "35.1.2.3/32")
    underlying_send = mocker.patch("requests.adapters.HTTPAdapter.send", return_value=Response())
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info("::ffff:35.1.2.3"),
    )
    init_internal_request_filter(cloud_outbound_request_policy())

    with pytest.raises(AirbyteTracedException):
        Session().send(_request("https://[::ffff:35.1.2.3]/"))

    underlying_send.assert_not_called()


def test_unrelated_mapped_public_ipv6_remains_allowed(mocker, monkeypatch):
    monkeypatch.setenv("AIRBYTE_SSRF_DENY_CIDRS", "35.1.2.3/32")
    underlying_send = mocker.patch("requests.adapters.HTTPAdapter.send", return_value=Response())
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info("::ffff:35.1.2.4"),
    )
    init_internal_request_filter(cloud_outbound_request_policy())

    response = Session().send(_request("https://[::ffff:35.1.2.4]/"))

    assert isinstance(response, Response)
    underlying_send.assert_called_once()


def test_invalid_configured_cidr_fails_policy_construction(monkeypatch):
    monkeypatch.setenv("AIRBYTE_SSRF_DENY_CIDRS", "not-a-network")

    with pytest.raises(ValueError, match="not-a-network"):
        cloud_outbound_request_policy()


def test_cname_target_is_checked_before_address_resolution(mocker):
    policy = cloud_outbound_request_policy(include_airbyte_service_names=True)
    init_internal_request_filter(policy)
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter._resolve_cname_chain",
        return_value=iter(["mcp.airbyte.com"]),
    )
    address_lookup = mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info("8.8.8.8"),
    )

    with pytest.raises(AirbyteTracedException) as exc_info:
        Session().send(_request("https://attacker-controlled.example/"))
    assert exc_info.value.internal_message
    assert "mcp.airbyte.com" in exc_info.value.internal_message
    address_lookup.assert_not_called()


def test_getaddrinfo_canonical_name_is_checked(mocker):
    policy = OutboundRequestPolicy(blocked_hosts=frozenset({"blocked.example"}))
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info("8.8.8.8", canonical_name="blocked.example."),
    )
    _reset_request_filter_policy_for_testing()
    init_internal_request_filter(policy)

    with pytest.raises(AirbyteTracedException) as exc_info:
        Session().send(_request("https://alias.example/"))
    assert exc_info.value.internal_message
    assert "blocked.example" in exc_info.value.internal_message


def test_cname_loop_is_rejected(mocker):
    first = Mock()
    first.target.to_text.return_value = "second.example."
    second = Mock()
    second.target.to_text.return_value = "first.example."
    resolver = Mock()
    resolver.resolve.side_effect = [[first], [second]]
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter._get_cname_resolver",
        return_value=resolver,
    )

    with pytest.raises(socket.gaierror, match="loop"):
        list(_resolve_cname_chain("first.example"))


def test_redirect_destination_is_revalidated(mocker):
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter.socket.getaddrinfo",
        return_value=_address_info("8.8.8.8"),
    )
    mocker.patch(
        "airbyte_cdk.utils.outbound_request_filter._resolve_cname_chain",
        return_value=iter(()),
    )
    init_internal_request_filter(cloud_outbound_request_policy(include_airbyte_service_names=True))

    adapter = requests_mock.Adapter()
    adapter.register_uri(
        "GET",
        "https://allowed.example/start",
        status_code=302,
        headers={"Location": "https://mcp.airbyte.com/secret"},
    )
    adapter.register_uri("GET", "https://mcp.airbyte.com/secret", text="should not be reached")
    session = Session()
    session.mount("https://", adapter)

    with pytest.raises(AirbyteTracedException) as exc_info:
        session.get("https://allowed.example/start")

    assert exc_info.value.internal_message
    assert "mcp.airbyte.com" in exc_info.value.internal_message
    assert [request.url for request in adapter.request_history] == ["https://allowed.example/start"]


def test_filter_installation_is_idempotent(mocker):
    underlying_send = mocker.patch.object(Session, "send", return_value=Response())
    init_internal_request_filter()
    init_internal_request_filter()

    Session().send(_request("https://8.8.8.8/"))

    underlying_send.assert_called_once()
