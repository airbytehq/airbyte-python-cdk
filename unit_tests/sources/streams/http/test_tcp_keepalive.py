#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

import logging
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock

import pytest
import urllib3.connection

from airbyte_cdk.sources.streams.http.http_client import HttpClient
from airbyte_cdk.sources.streams.http.tcp_keepalive import (
    TCP_KEEPALIVE_IDLE_SECONDS,
    TCP_KEEPALIVE_INTERVAL_SECONDS,
    TCP_KEEPALIVE_PROBE_COUNT,
    TcpKeepaliveHTTPAdapter,
    tcp_keepalive_socket_options,
)


@pytest.fixture(autouse=True)
def clear_options_cache():
    tcp_keepalive_socket_options.cache_clear()
    yield
    tcp_keepalive_socket_options.cache_clear()


def _ok_server():
    class OkHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), OkHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def _capture_socket_options(monkeypatch):
    captured_options = []
    real_connect = urllib3.connection.HTTPConnection.connect

    def capture_connect(connection):
        real_connect(connection)
        # The socket may be closed by the time the caller inspects it, so read
        # the options while the connection is still open.
        captured_options.append(
            (
                connection.sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE),
                connection.sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE),
            )
        )

    monkeypatch.setattr(urllib3.connection.HTTPConnection, "connect", capture_connect)
    return captured_options


def test_socket_options_returns_tuple():
    assert isinstance(tcp_keepalive_socket_options(), tuple)


@pytest.mark.skipif(not hasattr(socket, "TCP_KEEPIDLE"), reason="requires TCP_KEEPIDLE")
def test_socket_options_contains_keepalive_tuning():
    options = tcp_keepalive_socket_options()

    assert (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1) in options
    assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in options
    assert (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, TCP_KEEPALIVE_IDLE_SECONDS) in options
    assert (
        socket.IPPROTO_TCP,
        socket.TCP_KEEPINTVL,
        TCP_KEEPALIVE_INTERVAL_SECONDS,
    ) in options
    assert (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, TCP_KEEPALIVE_PROBE_COUNT) in options


def test_socket_options_degrade_when_idle_option_missing(monkeypatch):
    monkeypatch.delattr(socket, "TCP_KEEPIDLE", raising=False)
    monkeypatch.delattr(socket, "TCP_KEEPALIVE", raising=False)

    options = tcp_keepalive_socket_options()

    assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in options
    assert not any(value == TCP_KEEPALIVE_IDLE_SECONDS for _, _, value in options)
    if hasattr(socket, "TCP_KEEPINTVL"):
        assert (
            socket.IPPROTO_TCP,
            socket.TCP_KEEPINTVL,
            TCP_KEEPALIVE_INTERVAL_SECONDS,
        ) in options
    if hasattr(socket, "TCP_KEEPCNT"):
        assert (
            socket.IPPROTO_TCP,
            socket.TCP_KEEPCNT,
            TCP_KEEPALIVE_PROBE_COUNT,
        ) in options


def test_socket_options_drop_rejected_option(monkeypatch):
    if not hasattr(socket, "TCP_KEEPCNT"):
        pytest.skip("requires TCP_KEEPCNT")

    real_socket_cls = socket.socket

    class FakeSocket:
        def setsockopt(self, level, optname, value):
            if optname == socket.TCP_KEEPCNT:
                raise OSError("unsupported")

        def close(self):
            pass

    monkeypatch.setattr(socket, "socket", lambda *a, **kw: FakeSocket())

    options = tcp_keepalive_socket_options()

    assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in options
    assert not any(optname == socket.TCP_KEEPCNT for _, optname, _ in options)
    monkeypatch.undo()
    assert isinstance(real_socket_cls(socket.AF_INET, socket.SOCK_STREAM), real_socket_cls)


def test_socket_options_warn_when_keepalive_rejected(monkeypatch, caplog):
    class FakeSocket:
        def setsockopt(self, level, optname, value):
            if (level, optname) == (socket.SOL_SOCKET, socket.SO_KEEPALIVE):
                raise OSError("unsupported")

        def close(self):
            pass

    monkeypatch.setattr(socket, "socket", lambda *a, **kw: FakeSocket())

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        options = tcp_keepalive_socket_options()

    assert options == tuple(urllib3.connection.HTTPConnection.default_socket_options)
    assert "TCP keepalive is unavailable" in caplog.text


def test_socket_options_warn_when_probe_socket_fails(monkeypatch, caplog):
    def failing_socket(*args, **kwargs):
        raise OSError("no sockets")

    monkeypatch.setattr(socket, "socket", failing_socket)

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        options = tcp_keepalive_socket_options()

    assert options == tuple(urllib3.connection.HTTPConnection.default_socket_options)
    assert "TCP keepalive is unavailable" in caplog.text


def test_socket_options_do_not_warn_when_supported(caplog):
    with caplog.at_level(logging.WARNING, logger="airbyte"):
        tcp_keepalive_socket_options()

    assert "TCP keepalive is unavailable" not in caplog.text


def test_adapter_passes_options_to_pool_and_proxy_managers():
    adapter = TcpKeepaliveHTTPAdapter()

    expected = tcp_keepalive_socket_options()
    assert adapter.poolmanager.connection_pool_kw["socket_options"] == expected
    proxy_manager = adapter.proxy_manager_for("http://proxy.invalid:3128")
    assert proxy_manager.connection_pool_kw["socket_options"] == expected


@pytest.mark.skipif(not hasattr(socket, "TCP_KEEPIDLE"), reason="requires Linux")
def test_opted_in_http_client_connection_has_keepalive_enabled(monkeypatch):
    captured_options = _capture_socket_options(monkeypatch)

    server = _ok_server()
    try:
        http_client = HttpClient(name="test", logger=MagicMock(), use_tcp_keepalive=True)
        http_client.send_request(
            http_method="GET",
            url=f"http://127.0.0.1:{server.server_port}/",
            request_kwargs={"stream": True},
        )
        assert captured_options == [(1, TCP_KEEPALIVE_IDLE_SECONDS)]
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.skipif(not hasattr(socket, "TCP_KEEPIDLE"), reason="requires Linux")
def test_default_http_client_connection_has_no_keepalive(monkeypatch):
    captured_options = _capture_socket_options(monkeypatch)

    server = _ok_server()
    try:
        http_client = HttpClient(name="test", logger=MagicMock())
        http_client.send_request(
            http_method="GET",
            url=f"http://127.0.0.1:{server.server_port}/",
            request_kwargs={"stream": True},
        )
        assert captured_options
        assert captured_options[0][0] == 0
    finally:
        server.shutdown()
        server.server_close()
