#!/usr/bin/env bash
set -euo pipefail

REPRO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPRO_DIR
readonly ORBSTACK_SOCKET="unix:///Users/alex/.orbstack/run/docker.sock"
readonly IMAGE="airbyte/manifest-server:7.5.50"
readonly PRIVATE_NETWORK="agentic-2424-private"
readonly PUBLIC_NETWORK="agentic-2424-public-vip"
readonly CANARY_CONTAINER="agentic-2424-canary"
readonly MANIFEST_CONTAINER="agentic-2424-manifest-server"
readonly PRIVATE_IP="172.29.242.3"
readonly PUBLIC_IP="35.1.2.3"

export DOCKER_HOST="$ORBSTACK_SOCKET"

private_network_created=false
public_network_created=false
canary_created=false
manifest_created=false
temp_dir=""

cleanup() {
  if [[ "$manifest_created" == true ]]; then
    docker container rm --force "$MANIFEST_CONTAINER" >/dev/null 2>&1 || true
  fi
  if [[ "$canary_created" == true ]]; then
    docker container rm --force "$CANARY_CONTAINER" >/dev/null 2>&1 || true
  fi
  if [[ "$public_network_created" == true ]]; then
    docker network rm "$PUBLIC_NETWORK" >/dev/null 2>&1 || true
  fi
  if [[ "$private_network_created" == true ]]; then
    docker network rm "$PRIVATE_NETWORK" >/dev/null 2>&1 || true
  fi
  if [[ -n "$temp_dir" && -d "$temp_dir" ]]; then
    rm -f \
      "$temp_dir/ca.crt" \
      "$temp_dir/ca.key" \
      "$temp_dir/ca.srl" \
      "$temp_dir/server.crt" \
      "$temp_dir/server.csr" \
      "$temp_dir/server.key" \
      "$temp_dir/server.ext" \
      "$temp_dir/private-request.json" \
      "$temp_dir/private-envelope.json" \
      "$temp_dir/private-response.json" \
      "$temp_dir/public-request.json" \
      "$temp_dir/public-envelope.json" \
      "$temp_dir/public-response.json"
    rmdir "$temp_dir" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for resource in "$CANARY_CONTAINER" "$MANIFEST_CONTAINER"; do
  if docker container inspect "$resource" >/dev/null 2>&1; then
    echo "Refusing to reuse pre-existing container: $resource" >&2
    exit 1
  fi
done
for resource in "$PRIVATE_NETWORK" "$PUBLIC_NETWORK"; do
  if docker network inspect "$resource" >/dev/null 2>&1; then
    echo "Refusing to reuse pre-existing network: $resource" >&2
    exit 1
  fi
done

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/agentic-2424.XXXXXX")"
chmod 755 "$temp_dir"

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$temp_dir/ca.key" \
  -out "$temp_dir/ca.crt" \
  -days 1 \
  -subj "/CN=AGENTIC-2424 Lab CA" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes \
  -keyout "$temp_dir/server.key" \
  -out "$temp_dir/server.csr" \
  -subj "/CN=$PUBLIC_IP" >/dev/null 2>&1
printf '%s\n' \
  'basicConstraints=CA:FALSE' \
  'keyUsage=digitalSignature,keyEncipherment' \
  'extendedKeyUsage=serverAuth' \
  "subjectAltName=IP:$PRIVATE_IP,IP:$PUBLIC_IP" >"$temp_dir/server.ext"
openssl x509 -req \
  -in "$temp_dir/server.csr" \
  -CA "$temp_dir/ca.crt" \
  -CAkey "$temp_dir/ca.key" \
  -CAcreateserial \
  -out "$temp_dir/server.crt" \
  -days 1 \
  -extfile "$temp_dir/server.ext" >/dev/null 2>&1
chmod 644 "$temp_dir/ca.crt" "$temp_dir/server.crt" "$temp_dir/server.key"

docker network create --internal --subnet 172.29.242.0/24 "$PRIVATE_NETWORK" >/dev/null
private_network_created=true
docker network create --internal --subnet 35.1.2.0/24 "$PUBLIC_NETWORK" >/dev/null
public_network_created=true

for network in "$PRIVATE_NETWORK" "$PUBLIC_NETWORK"; do
  if [[ "$(docker network inspect "$network" --format '{{.Internal}}')" != "true" ]]; then
    echo "Safety check failed: network is not internal: $network" >&2
    exit 1
  fi
done

docker run --detach \
  --platform linux/amd64 \
  --name "$CANARY_CONTAINER" \
  --network "$PRIVATE_NETWORK" \
  --ip "$PRIVATE_IP" \
  --mount "type=bind,src=$REPRO_DIR/canary.py,dst=/repro/canary.py,readonly" \
  --mount "type=bind,src=$temp_dir,dst=/cert,readonly" \
  "$IMAGE" \
  python /repro/canary.py >/dev/null
canary_created=true
docker network connect --ip "$PUBLIC_IP" "$PUBLIC_NETWORK" "$CANARY_CONTAINER"

for _ in $(seq 1 60); do
  if docker logs "$CANARY_CONTAINER" 2>&1 | grep -q '^READY 8443$'; then
    break
  fi
  sleep 0.5
done
if ! docker logs "$CANARY_CONTAINER" 2>&1 | grep -q '^READY 8443$'; then
  docker logs "$CANARY_CONTAINER" >&2
  echo "Canary did not become ready" >&2
  exit 1
fi

docker run --detach \
  --platform linux/amd64 \
  --name "$MANIFEST_CONTAINER" \
  --network "$PRIVATE_NETWORK" \
  --ip 172.29.242.2 \
  --env AIRBYTE_EDITION=cloud \
  --env DEPLOYMENT_MODE=cloud \
  --env REQUESTS_CA_BUNDLE=/cert/ca.crt \
  --env HTTP_PROXY= \
  --env HTTPS_PROXY= \
  --env ALL_PROXY= \
  --env NO_PROXY='*' \
  --mount "type=bind,src=$temp_dir,dst=/cert,readonly" \
  --mount "type=bind,src=$temp_dir/ca.crt,dst=/usr/local/lib/python3.13/site-packages/certifi/cacert.pem,readonly" \
  "$IMAGE" >/dev/null
manifest_created=true
docker network connect --ip 35.1.2.2 "$PUBLIC_NETWORK" "$MANIFEST_CONTAINER"

for _ in $(seq 1 120); do
  if docker exec "$MANIFEST_CONTAINER" python -c \
    'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8080/health", timeout=1).read()' \
    >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if ! docker exec "$MANIFEST_CONTAINER" python -c \
  'import urllib.request; urllib.request.urlopen("http://127.0.0.1:8080/health", timeout=1).read()' \
  >/dev/null 2>&1; then
  docker logs "$MANIFEST_CONTAINER" >&2
  echo "Manifest server did not become ready" >&2
  exit 1
fi

echo "SAFETY network=$PRIVATE_NETWORK internal=$(docker network inspect "$PRIVATE_NETWORK" --format '{{.Internal}}') subnet=172.29.242.0/24"
echo "SAFETY network=$PUBLIC_NETWORK internal=$(docker network inspect "$PUBLIC_NETWORK" --format '{{.Internal}}') subnet=35.1.2.0/24"
docker exec "$MANIFEST_CONTAINER" python -c \
  "import ipaddress; print('CLASSIFY private=$PRIVATE_IP is_private=' + str(ipaddress.ip_address('$PRIVATE_IP').is_private)); print('CLASSIFY public_vip=$PUBLIC_IP is_private=' + str(ipaddress.ip_address('$PUBLIC_IP').is_private))"

jq --arg target "https://$PRIVATE_IP:8443" \
  '.manifest.streams[0].retriever.requester.url = ($target + "/records")' \
  "$REPRO_DIR/request.json" >"$temp_dir/private-request.json"
hits_before_private="$(docker logs "$CANARY_CONTAINER" 2>&1 | awk '/^HIT / {count++} END {print count+0}')"
readonly hits_before_private
docker exec --interactive "$MANIFEST_CONTAINER" python -c '
import json
import sys
import urllib.error
import urllib.request

request = urllib.request.Request(
    "http://127.0.0.1:8080/v1/manifest/test_read",
    data=sys.stdin.buffer.read(),
    headers={"Content-Type": "application/json"},
)
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        status = response.status
        body = response.read().decode()
except urllib.error.HTTPError as error:
    status = error.code
    body = error.read().decode()
print(json.dumps({"status": status, "body": json.loads(body)}))
' <"$temp_dir/private-request.json" >"$temp_dir/private-envelope.json"
private_status="$(jq -r '.status' "$temp_dir/private-envelope.json")"
jq '.body' "$temp_dir/private-envelope.json" >"$temp_dir/private-response.json"
hits_after_private="$(docker logs "$CANARY_CONTAINER" 2>&1 | awk '/^HIT / {count++} END {print count+0}')"
readonly hits_after_private

if [[ "$hits_after_private" != "$hits_before_private" ]]; then
  echo "Private control unexpectedly reached the canary" >&2
  exit 1
fi
if ! grep -q 'private network' "$temp_dir/private-response.json"; then
  echo "Private control did not contain the expected rejection" >&2
  jq . "$temp_dir/private-response.json" >&2
  exit 1
fi

jq --arg target "https://$PUBLIC_IP:8443" \
  '.manifest.streams[0].retriever.requester.url = ($target + "/records")' \
  "$REPRO_DIR/request.json" >"$temp_dir/public-request.json"
docker exec --interactive "$MANIFEST_CONTAINER" python -c '
import json
import sys
import urllib.error
import urllib.request

request = urllib.request.Request(
    "http://127.0.0.1:8080/v1/manifest/test_read",
    data=sys.stdin.buffer.read(),
    headers={"Content-Type": "application/json"},
)
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        status = response.status
        body = response.read().decode()
except urllib.error.HTTPError as error:
    status = error.code
    body = error.read().decode()
print(json.dumps({"status": status, "body": json.loads(body)}))
' <"$temp_dir/public-request.json" >"$temp_dir/public-envelope.json"
public_status="$(jq -r '.status' "$temp_dir/public-envelope.json")"
jq '.body' "$temp_dir/public-envelope.json" >"$temp_dir/public-response.json"
hits_after_public="$(docker logs "$CANARY_CONTAINER" 2>&1 | awk '/^HIT / {count++} END {print count+0}')"
readonly hits_after_public

if [[ "$public_status" != "200" ]]; then
  echo "Public-VIP request failed with HTTP $public_status" >&2
  jq . "$temp_dir/public-response.json" >&2
  exit 1
fi
if ! jq -e '.. | strings | select(. == "AGENTIC_2424_CANARY")' "$temp_dir/public-response.json" >/dev/null; then
  echo "Public-VIP response did not contain the canary nonce" >&2
  jq . "$temp_dir/public-response.json" >&2
  exit 1
fi
if (( hits_after_public <= hits_after_private )); then
  echo "Public-VIP request did not reach the canary" >&2
  exit 1
fi

private_excerpt="$(jq -c '{logs: [.logs[]? | {level, message}], slices: (.slices | length)}' "$temp_dir/private-response.json")"
readonly private_excerpt
public_excerpt="$(jq -c '{records: [.slices[]?.pages[]?.records[]?]}' "$temp_dir/public-response.json")"
readonly public_excerpt

echo "RESULT private_http=$private_status canary_hits_before=$hits_before_private canary_hits_after=$hits_after_private"
echo "PRIVATE_RESPONSE $private_excerpt"
echo "RESULT public_vip_http=$public_status canary_hits_after=$hits_after_public"
echo "PUBLIC_RESPONSE $public_excerpt"

cleanup
trap - EXIT

if docker container inspect "$CANARY_CONTAINER" >/dev/null 2>&1 || \
  docker container inspect "$MANIFEST_CONTAINER" >/dev/null 2>&1 || \
  docker network inspect "$PRIVATE_NETWORK" >/dev/null 2>&1 || \
  docker network inspect "$PUBLIC_NETWORK" >/dev/null 2>&1; then
  echo "Cleanup verification failed" >&2
  exit 1
fi
echo "CLEANUP no owned containers or networks remain"
