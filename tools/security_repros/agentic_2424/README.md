# AGENTIC-2424 manifest SSRF reproduction

This disposable harness demonstrates the reported trust-boundary gap without
contacting Kubernetes, production, staging, cloud metadata, or any external
endpoint.

It runs the released `airbyte/manifest-server:7.5.50` image and one synthetic
HTTPS canary on two Docker networks:

- `172.29.242.0/24`, representing a private service address.
- `35.1.2.0/24`, representing the same service through a synthetic public or
  load-balancer-style address.

Both networks are created with Docker's `--internal` flag. The script verifies
that property before making a request, so neither container has an external
network path. The canary contains only the nonce `AGENTIC_2424_CANARY`.

The crafted manifest is identical for both requests except for the target in
`url`:

```yaml
version: 6.48.15
type: DeclarativeSource
check:
  type: CheckStream
  stream_names: [canary]
spec:
  type: Spec
  connection_specification:
    type: object
    properties: {}
    additionalProperties: true
streams:
  - type: DeclarativeStream
    name: canary
    primary_key: [id]
    retriever:
      type: SimpleRetriever
      requester:
        type: HttpRequester
        url: https://TARGET:8443/records
        http_method: GET
      record_selector:
        type: RecordSelector
        extractor:
          type: DpathExtractor
          field_path: [records]
```

The script calls `/v1/manifest/test_read` twice:

1. `https://172.29.242.3:8443` must be rejected as private, with no canary hit.
2. `https://35.1.2.3:8443` must pass the current `is_private` test, reach the
   same canary, and return the nonce as a Builder record.

## Run

OrbStack must be running and the released image must be available locally.

```bash
./tools/security_repros/agentic_2424/run.sh
```

The script always uses this Docker socket explicitly:

```text
unix:///Users/alex/.orbstack/run/docker.sock
```

It refuses to reuse pre-existing resources with its names. An exit trap removes
only the containers and networks created by the run. A successful run also
verifies that no owned Docker resource remains.

## What this proves

It proves the manifest is the caller-controlled request primitive and that the
CDK's address classifier makes its decision from the apparent destination IP.
The same internal canary is denied at an RFC1918 address and reachable when
presented at an address classified as public.

This is a faithful network-boundary simulation, not evidence that a particular
production load balancer or service is currently exposed. Establishing deployed
impact separately requires an owned canary behind the real stage network path.
