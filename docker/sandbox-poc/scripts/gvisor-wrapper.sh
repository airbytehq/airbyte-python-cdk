#!/bin/bash
# gVisor wrapper for source-declarative-manifest
COMMAND="$1"
shift

# Use pre-created OCI bundle directory
BUNDLE_DIR="/var/run/oci-bundle"

# Run the command with runsc
cd $BUNDLE_DIR
runsc -TESTONLY-unsafe-nonroot run --bundle=$BUNDLE_DIR container1 || python /airbyte/integration_code/main.py "$COMMAND" "$@"
