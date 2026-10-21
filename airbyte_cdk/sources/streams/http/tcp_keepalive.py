#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import functools
import logging
import socket
from typing import List, Tuple, cast

import requests
import urllib3.connection

TCP_KEEPALIVE_IDLE_SECONDS = 60
TCP_KEEPALIVE_INTERVAL_SECONDS = 10
TCP_KEEPALIVE_PROBE_COUNT = 6

logger = logging.getLogger("airbyte")


@functools.lru_cache(maxsize=None)
def tcp_keepalive_socket_options() -> Tuple[Tuple[int, int, int], ...]:
    """Socket options enabling TCP keepalive, probed for what this OS supports.

    Returns urllib3's default options plus SO_KEEPALIVE and the platform's idle /
    interval / probe-count tuning options (TCP_KEEPIDLE on Linux/Windows,
    TCP_KEEPALIVE on macOS). Each option is tried once on a throwaway socket and
    dropped if the OS rejects it, because urllib3 applies these at connect time
    and an unsupported option would fail every connection. Never raises.
    """
    base_options = cast(
        List[Tuple[int, int, int]],
        list(urllib3.connection.HTTPConnection.default_socket_options),
    )

    idle_option = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE", None)
    candidates: List[Tuple[int, int, int]] = [
        (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    ]
    if idle_option is not None:
        candidates.append((socket.IPPROTO_TCP, idle_option, TCP_KEEPALIVE_IDLE_SECONDS))
    for name, value in (
        ("TCP_KEEPINTVL", TCP_KEEPALIVE_INTERVAL_SECONDS),
        ("TCP_KEEPCNT", TCP_KEEPALIVE_PROBE_COUNT),
    ):
        option = getattr(socket, name, None)
        if option is not None:
            candidates.append((socket.IPPROTO_TCP, option, value))

    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except OSError as error:
        _warn_keepalive_unavailable(error)
        return tuple(base_options)
    try:
        supported = base_options
        for level, optname, value in candidates:
            try:
                probe.setsockopt(level, optname, value)
            except OSError as error:
                if (level, optname) == (socket.SOL_SOCKET, socket.SO_KEEPALIVE):
                    # Without SO_KEEPALIVE the tuning options are useless.
                    _warn_keepalive_unavailable(error)
                    return tuple(base_options)
                continue
            supported.append((level, optname, value))
        return tuple(supported)
    finally:
        probe.close()


def _warn_keepalive_unavailable(error: OSError) -> None:
    # Logged once per process because tcp_keepalive_socket_options() is cached.
    logger.warning(
        "TCP keepalive is unavailable on this platform, so connections use plain sockets: %s", error
    )


class TcpKeepaliveHTTPAdapter(requests.adapters.HTTPAdapter):
    """HTTPAdapter that enables TCP keepalive on every connection it pools."""

    def init_poolmanager(self, *args, **pool_kwargs):  # type: ignore[no-untyped-def]
        pool_kwargs.setdefault("socket_options", tcp_keepalive_socket_options())
        super().init_poolmanager(*args, **pool_kwargs)  # type: ignore[no-untyped-call]

    def proxy_manager_for(self, proxy, **proxy_kwargs):  # type: ignore[no-untyped-def]
        # requests does not forward init_poolmanager kwargs to proxy managers,
        # so without this override proxied connections get no keepalive.
        proxy_kwargs.setdefault("socket_options", tcp_keepalive_socket_options())
        return super().proxy_manager_for(proxy, **proxy_kwargs)  # type: ignore[no-untyped-call]
