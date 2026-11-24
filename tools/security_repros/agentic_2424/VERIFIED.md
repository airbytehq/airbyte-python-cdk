# Verified result

Observed at `2026-11-24T02:09:33Z` from CDK commit
`4934b6957c9e994225cfac090bdafaf73b5d3451`.

Image:

```text
airbyte/manifest-server:7.5.50
airbyte/manifest-server@sha256:545bea0f3707ad3e3a269337922b6f251b0892d6fa93bc888e92e86bb5d66a42
linux/amd64
```

Command:

```bash
./tools/security_repros/agentic_2424/run.sh
```

Observed output:

```text
SAFETY network=agentic-2424-private internal=true subnet=172.29.242.0/24
SAFETY network=agentic-2424-public-vip internal=true subnet=35.1.2.0/24
CLASSIFY private=172.29.242.3 is_private=True
CLASSIFY public_vip=35.1.2.3 is_private=False
RESULT private_http=200 canary_hits_before=0 canary_hits_after=0
PRIVATE_RESPONSE {"logs":[{"level":"ERROR","message":"Invalid URL endpoint: The endpoint that data is being requested from belongs to a private network. Source connectors only support requesting data from public API endpoints."}],"slices":0}
RESULT public_vip_http=200 canary_hits_after=1
PUBLIC_RESPONSE {"records":[{"id":1,"nonce":"AGENTIC_2424_CANARY"}]}
CLEANUP no owned containers or networks remain
```

The test-read endpoint represents a blocked stream read as an HTTP 200 response
containing an error log, so the private control's meaningful assertions are the
private-network error, zero slices, and no canary hit.

The public-VIP run used the same manifest and same canary. Only the requester's
target changed. It produced one canary hit and returned the synthetic nonce as a
record.

After the run, Docker reported no containers or networks matching the harness's
owned names.

## Limit

This verifies the request-filter bypass class under the released manifest-server
image. The synthetic public subnet was intentionally isolated from the internet.
It does not establish that a particular deployed Airbyte load balancer currently
routes to an unauthenticated or sensitive backend.
