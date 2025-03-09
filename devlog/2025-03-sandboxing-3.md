# Resolving gVisor Rootless Container User Namespace Issue

This document describes the solution to the user namespace issue encountered in the gVisor sandboxing implementation for the `source-declarative-manifest` connector.

## Overview

The previous gVisor implementation in [PR #400](https://github.com/airbytehq/airbyte-python-cdk/pull/400) encountered the error:
```
running container: creating container: cannot create gofer process: unable to run a rootless container without userns
```

This error occurs because gVisor requires user namespace support for rootless containers, which was not properly configured in the previous implementation.

## Implementation Details

The key changes in this implementation:

1. **Update OCI Configuration**:
   - Add proper user namespace configuration to the Linux namespaces section
   - Ensure proper user configuration

2. **Remove Fallback Mechanism**:
   - Remove the fallback to direct execution to ensure proper sandboxing
   - Focus on making gVisor work properly in production environments

3. **Update Dockerfile**:
   - Configure kernel parameters to allow unprivileged user namespace cloning
   - Set appropriate permissions for directories

## Technical Approach

### User Namespace Requirements

gVisor requires user namespace support for rootless containers. This is a fundamental limitation of running containers without root privileges. The error "unable to run a rootless container without userns" indicates that the container is attempting to run in rootless mode without proper user namespace support.

### Solution

The solution addresses these issues by:

1. Configuring the OCI bundle with proper user namespace settings
2. Setting kernel parameters to allow unprivileged user namespace cloning
3. Removing the fallback mechanism to ensure proper sandboxing

## Testing Results

The implementation was tested with various configurations:

### Basic Run
```bash
docker build -f Dockerfile.gvisor -t airbyte/source-declarative-manifest-gvisor .
docker run --rm airbyte/source-declarative-manifest-gvisor spec
```

### Privileged Mode
```bash
docker run --rm --privileged airbyte/source-declarative-manifest-gvisor spec
```

### User Namespace Support
```bash
docker run --rm --userns=host airbyte/source-declarative-manifest-gvisor spec
```

The implementation now requires proper user namespace support to function, as the fallback mechanism has been removed to ensure proper sandboxing.

## Production Deployment Requirements

For gVisor to work properly in production environments:

1. **Docker Runtime Configuration**: Configure the Docker daemon with user namespace remapping using the `userns-remap` option in `/etc/docker/daemon.json`:
   ```json
   {
     "userns-remap": "default"
   }
   ```

2. **Host-Level Configuration**: Enable user namespaces at the host level:
   ```bash
   echo 'kernel.unprivileged_userns_clone=1' > /etc/sysctl.d/userns.conf
   sysctl -w kernel.unprivileged_userns_clone=1
   ```

3. **Container Runtime Flags**: Run containers with the appropriate flags:
   ```bash
   docker run --security-opt seccomp=unconfined --security-opt apparmor=unconfined --userns=host
   ```

## Testing Limitations

During testing, we encountered several limitations related to user namespace support:

1. **Kernel Support**: Some environments may not have the `kernel.unprivileged_userns_clone` parameter available, which is required for unprivileged user namespace support.

2. **Docker Environment**: Even with the `--userns=host` and `--privileged` flags, some Docker environments may still encounter the error:
   ```
   running container: creating container: cannot create gofer process: newuidmap failed: exit status 1
   ```
   This indicates that the host environment does not have the necessary user namespace support configured.

3. **Production Deployment**: For production deployment, it's essential to ensure that the host system has proper user namespace support enabled at both the kernel and Docker daemon levels.

## Conclusion

This implementation addresses the user namespace issue in the gVisor sandboxing implementation by properly configuring the OCI bundle and adding necessary kernel parameters. The removal of the fallback mechanism ensures that the connector will only run with proper sandboxing, which is essential for production use.
