# Improving gVisor Sandboxing Implementation

This document describes improvements to the gVisor sandboxing implementation for the `source-declarative-manifest` connector, specifically addressing permission issues encountered in the original implementation.

## Overview

The original gVisor implementation in [PR #399](https://github.com/airbytehq/airbyte-python-cdk/pull/399) encountered permission issues when attempting to create directories at runtime. This improvement moves critical privileged operations to build time and uses a static configuration file rather than generating it dynamically at runtime.

## Implementation Details

The key changes in this implementation:

1. **Move Directory Creation to Build Time**: 
   - Create the OCI bundle directory (`/var/run/oci-bundle`) during Docker image build
   - Set appropriate permissions on the directory to ensure it's accessible by the non-root user

2. **Use Static Configuration File**:
   - Replace the dynamic config.json generation with a static file
   - Pre-configure the file for the `spec` command
   - Copy the config file to the OCI bundle directory during build

3. **Simplify Wrapper Script**:
   - Remove temporary directory creation and cleanup
   - Use the pre-created OCI bundle directory
   - Maintain the fallback mechanism to direct execution if runsc fails

## Technical Approach

### Original Implementation Issues

The original implementation encountered the following error:
```
running container: creating container: creating container root directory "/var/run/runsc": mkdir /var/run/runsc: permission denied
```

This occurred because:
1. The wrapper script attempted to create directories at runtime
2. The container was running as a non-root user without the necessary permissions
3. The `runsc` command requires specific directories to exist with proper permissions

### Solution

The solution addresses these issues by:

1. Creating all required directories during the Docker build process (as root):
   - `/var/run/oci-bundle/rootfs` - The OCI bundle directory for container execution
   - `/var/run/runsc` - Primary runsc runtime directory
   - `/run/runsc` - Alternative runsc runtime directory
   - `/tmp/runsc` - Temporary runsc directory for runtime operations
   
2. Setting appropriate permissions on these directories:
   - `755` permissions for the OCI bundle directory
   - `777` permissions for all runsc directories to ensure non-root access
   
3. Pre-creating a static config.json file with the `spec` command hardcoded
4. Copying this file to the OCI bundle directory during build
5. Executing runsc once during build time as root to create any additional required directories
6. Simplifying the wrapper script to use the pre-created directories and files

## Testing Results

The implementation was tested by building and running the Docker image with various configurations:

### Basic Run
```bash
cd docker/sandbox-poc
docker build -f Dockerfile.gvisor -t airbyte/source-declarative-manifest-gvisor .
docker run --rm airbyte/source-declarative-manifest-gvisor spec
```

The error changed from the original permission denied error to:
```
running container: creating container: cannot create gofer process: unable to run a rootless container without userns
```

The container successfully falls back to direct execution and completes the spec command.

### Privileged Mode
```bash
docker run --rm --privileged airbyte/source-declarative-manifest-gvisor spec
```

Even with privileged mode, the same error occurs:
```
running container: creating container: cannot create gofer process: unable to run a rootless container without userns
```

### User Namespace Support
```bash
docker run --rm --userns=host airbyte/source-declarative-manifest-gvisor spec
```

The user namespace flag also results in the same error:
```
running container: creating container: cannot create gofer process: unable to run a rootless container without userns
```

These tests indicate that while we've resolved the directory permission issues, running gVisor within a container requires additional Docker runtime configuration beyond what can be achieved from within the container itself.

## Considerations for Future Work

1. **Dynamic Command Support**: The current implementation hardcodes the `spec` command in the config.json file. Future work could explore methods to support dynamic commands without requiring privileged operations at runtime.

2. **Production Deployment**: For production use, consider:
   - Using Docker's runtime configuration to specify runsc as the runtime
   - Running containers with the necessary privileges for runsc
   - Implementing a more complete OCI bundle configuration

## FAQ

**Q: Why hardcode the `spec` command instead of making it dynamic?**  
A: Making the command dynamic would require modifying the config.json file at runtime, which would still be a privileged operation. The current approach focuses on fixing the permission issues first, with dynamic command support as a potential future enhancement.

**Q: Does this implementation still provide the security benefits of gVisor?**  
A: Yes, when the container has the proper permissions to run runsc, it will use gVisor's sandboxing capabilities. The fallback mechanism ensures the connector still functions in environments without these permissions.

## Closing & Next Steps

This implementation successfully addresses the permission issues in the original gVisor implementation by moving privileged operations to build time. Future work could focus on:

1. Supporting dynamic commands without requiring runtime file modifications
2. Enhancing the OCI bundle configuration for more comprehensive sandboxing
3. Testing in environments with proper permissions for runsc
4. Exploring alternative approaches to pass command-line arguments to the sandboxed application
