import json
import ssl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


NONCE = "AGENTIC_2424_CANARY"


class CanaryHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        print(f"HIT {self.path}", flush=True)
        if self.path != "/records":
            self.send_error(404)
            return

        body = json.dumps({"records": [{"id": 1, "nonce": NONCE}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


server = ThreadingHTTPServer(("0.0.0.0", 8443), CanaryHandler)
tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
tls.load_cert_chain("/cert/server.crt", "/cert/server.key")
server.socket = tls.wrap_socket(server.socket, server_side=True)
print("READY 8443", flush=True)
server.serve_forever()
