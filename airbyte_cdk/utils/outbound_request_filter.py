# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

"""Process-wide protection for outbound Requests traffic in Airbyte Cloud."""

from __future__ import annotations

import ipaddress
import os
import socket
import threading
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, FrozenSet, Iterable, Tuple
from urllib.parse import urlparse

import dns.exception
import dns.resolver
import requests
from requests import PreparedRequest, Response, Session

from airbyte_cdk.models import FailureType
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

VALID_URL_SCHEMES = frozenset({"https"})
_INTERNAL_REQUEST_FILTER_MARKER = "_airbyte_internal_request_filter"
_INTERNAL_REQUEST_FILTER_LOCK = threading.Lock()
_CNAME_RESOLVER_LOCK = threading.Lock()
_MAX_CNAME_DEPTH = 16
_CNAME_RESOLVER: dns.resolver.Resolver | None = None

_MANIFEST_SERVER_BLOCKED_HOSTS = frozenset(
    {
        "api.airbyte.com",
        "cloud.airbyte.com",
        "cloud.airbyte.io",
        "connectors.airbyte.com",
        "mcp.airbyte.com",
    }
)


@dataclass(frozen=True)
class OutboundRequestPolicy:
    """Additional public destinations denied on top of non-global IP addresses."""

    blocked_hosts: FrozenSet[str] = field(default_factory=frozenset)
    blocked_suffixes: FrozenSet[str] = field(default_factory=frozenset)
    blocked_networks: Tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...] = ()
    inspect_cname_chain: bool = False

    def merge(self, other: "OutboundRequestPolicy") -> "OutboundRequestPolicy":
        networks = {str(network): network for network in self.blocked_networks}
        networks.update({str(network): network for network in other.blocked_networks})
        return OutboundRequestPolicy(
            blocked_hosts=self.blocked_hosts | other.blocked_hosts,
            blocked_suffixes=self.blocked_suffixes | other.blocked_suffixes,
            blocked_networks=tuple(networks[key] for key in sorted(networks)),
            inspect_cname_chain=self.inspect_cname_chain or other.inspect_cname_chain,
        )


_ACTIVE_POLICY = OutboundRequestPolicy()


def cloud_outbound_request_policy(
    *, include_airbyte_service_names: bool = False
) -> OutboundRequestPolicy:
    """Build the Cloud policy, including additive deployment-specific denies."""

    hosts = _split_env("AIRBYTE_SSRF_DENY_HOSTS")
    suffixes = _split_env("AIRBYTE_SSRF_DENY_HOST_SUFFIXES")
    if include_airbyte_service_names:
        hosts.update(_MANIFEST_SERVER_BLOCKED_HOSTS)

    normalized_hosts = frozenset(_normalize_hostname(host) for host in hosts)
    normalized_suffixes = frozenset(_normalize_hostname(suffix.lstrip(".")) for suffix in suffixes)
    networks = tuple(
        ipaddress.ip_network(value, strict=False)
        for value in sorted(_split_env("AIRBYTE_SSRF_DENY_CIDRS"))
    )
    return OutboundRequestPolicy(
        blocked_hosts=normalized_hosts,
        blocked_suffixes=normalized_suffixes,
        blocked_networks=networks,
        inspect_cname_chain=include_airbyte_service_names
        or bool(normalized_hosts or normalized_suffixes),
    )


def init_internal_request_filter(policy: OutboundRequestPolicy | None = None) -> None:
    """Install the process-wide Requests filter once and add the supplied policy."""

    global _ACTIVE_POLICY

    with _INTERNAL_REQUEST_FILTER_LOCK:
        _ACTIVE_POLICY = _ACTIVE_POLICY.merge(policy or OutboundRequestPolicy())
        if not getattr(Session.send, _INTERNAL_REQUEST_FILTER_MARKER, False):
            _wrap_session_send()


def _wrap_session_send() -> None:
    wrapped_fn = Session.send

    @wraps(wrapped_fn)
    def filtered_send(self: Any, request: PreparedRequest, **kwargs: Any) -> Response:
        _validate_prepared_request(request)
        return wrapped_fn(self, request, **kwargs)

    setattr(filtered_send, _INTERNAL_REQUEST_FILTER_MARKER, True)
    Session.send = filtered_send  # type: ignore[method-assign]


def _validate_prepared_request(request: PreparedRequest) -> None:
    parsed_url = urlparse(request.url)

    if parsed_url.scheme not in VALID_URL_SCHEMES:
        raise requests.exceptions.InvalidSchema(
            "Invalid Protocol Scheme: The endpoint that data is being requested from is using an "
            f"invalid or insecure protocol {parsed_url.scheme!r}. Valid protocol schemes: "
            f"{','.join(sorted(VALID_URL_SCHEMES))}"
        )

    raw_hostname = parsed_url.hostname
    if not raw_hostname or not isinstance(raw_hostname, str):
        raise requests.exceptions.InvalidURL(
            "Invalid URL specified: The endpoint that data is being requested from is not a valid URL"
        )

    try:
        hostname = _normalize_hostname(raw_hostname)
        _reject_blocked_hostname(hostname, _ACTIVE_POLICY)

        if _ACTIVE_POLICY.inspect_cname_chain and not _is_ip_literal(hostname):
            for cname in _resolve_cname_chain(hostname):
                _reject_blocked_hostname(cname, _ACTIVE_POLICY)

        address_info_entries = socket.getaddrinfo(
            hostname,
            parsed_url.port,
            flags=socket.AI_CANONNAME,
        )
        for entry in address_info_entries:
            canonical_name = entry[3]
            if canonical_name:
                _reject_blocked_hostname(_normalize_hostname(canonical_name), _ACTIVE_POLICY)
            _reject_blocked_ip(ipaddress.ip_address(entry[4][0]), _ACTIVE_POLICY)
    except (socket.gaierror, UnicodeError, ValueError) as exception:
        raise requests.exceptions.InvalidURL(
            f"Invalid URL {parsed_url}: {exception}"
        ) from exception


def _reject_blocked_hostname(hostname: str, policy: OutboundRequestPolicy) -> None:
    if hostname in policy.blocked_hosts or any(
        hostname == suffix or hostname.endswith(f".{suffix}") for suffix in policy.blocked_suffixes
    ):
        _raise_blocked_destination(hostname, "hostname")


def _reject_blocked_ip(
    address: ipaddress.IPv4Address | ipaddress.IPv6Address,
    policy: OutboundRequestPolicy,
) -> None:
    if (
        not address.is_global
        or address.is_multicast
        or any(address in network for network in policy.blocked_networks)
    ):
        _raise_blocked_destination(str(address), "address")


def _raise_blocked_destination(destination: str, kind: str) -> None:
    raise AirbyteTracedException(
        internal_message=f"Invalid URL endpoint: {kind} {destination!r} is not permitted",
        failure_type=FailureType.config_error,
        message=(
            "Invalid URL endpoint: The endpoint that data is being requested from is not a "
            "permitted public API endpoint."
        ),
    )


def _resolve_cname_chain(hostname: str) -> Iterable[str]:
    resolver = _get_cname_resolver()
    current = hostname
    visited = {current}

    for _ in range(_MAX_CNAME_DEPTH):
        try:
            answer = resolver.resolve(current, "CNAME", search=False)
        except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
            return
        except (dns.exception.Timeout, dns.resolver.NoNameservers) as exception:
            raise socket.gaierror(
                f"DNS CNAME lookup failed for {current!r}: {exception}"
            ) from exception

        if len(answer) != 1:
            raise socket.gaierror(f"DNS returned an invalid CNAME response for {current!r}")

        target = _normalize_hostname(answer[0].target.to_text())
        if target in visited:
            raise socket.gaierror(f"DNS CNAME loop detected for {hostname!r}")
        visited.add(target)
        yield target
        current = target

    raise socket.gaierror(f"DNS CNAME chain for {hostname!r} exceeds {_MAX_CNAME_DEPTH} records")


def _get_cname_resolver() -> dns.resolver.Resolver:
    global _CNAME_RESOLVER
    with _CNAME_RESOLVER_LOCK:
        if _CNAME_RESOLVER is None:
            _CNAME_RESOLVER = dns.resolver.Resolver()
            _CNAME_RESOLVER.cache = dns.resolver.LRUCache(max_size=1024)
        return _CNAME_RESOLVER


def _normalize_hostname(hostname: str) -> str:
    normalized = hostname.rstrip(".").lower()
    if not normalized:
        raise ValueError("hostname is empty")
    if _is_ip_literal(normalized):
        return normalized
    return normalized.encode("idna").decode("ascii")


def _is_ip_literal(hostname: str) -> bool:
    try:
        ipaddress.ip_address(hostname)
        return True
    except ValueError:
        return False


def _split_env(name: str) -> set[str]:
    return {value.strip() for value in os.getenv(name, "").split(",") if value.strip()}


def _reset_request_filter_policy_for_testing() -> None:
    """Reset additive policy state. Tests must also patch Session.send to remove the wrapper."""

    global _ACTIVE_POLICY
    with _INTERNAL_REQUEST_FILTER_LOCK:
        _ACTIVE_POLICY = OutboundRequestPolicy()
