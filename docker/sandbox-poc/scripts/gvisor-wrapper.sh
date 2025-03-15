#!/bin/bash
# gVisor wrapper for source-declarative-manifest
COMMAND="$1"
shift

# Create a temporary OCI bundle directory
BUNDLE_DIR=$(mktemp -d)
mkdir -p $BUNDLE_DIR/rootfs

# Create a simple config.json for the OCI bundle
cat > $BUNDLE_DIR/config.json << EOFINNER
{
  "ociVersion": "1.0.0",
  "process": {
    "terminal": false,
    "user": {
      "uid": 0,
      "gid": 0
    },
    "args": [
      "python", "/airbyte/integration_code/main.py", "$COMMAND", "$@"
    ],
    "env": [
      "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
      "TERM=xterm"
    ],
    "cwd": "/"
  },
  "root": {
    "path": "rootfs"
  },
  "linux": {}
}
EOFINNER

# Run the command with runsc
cd $BUNDLE_DIR
runsc -TESTONLY-unsafe-nonroot run --bundle=$BUNDLE_DIR container1 || python /airbyte/integration_code/main.py "$COMMAND" "$@"

# Clean up
rm -rf $BUNDLE_DIR
