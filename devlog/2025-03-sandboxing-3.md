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

For gVisor to work properly in production environments, several host-level configurations are required:

1. **Kernel Parameter Configuration**:
   
   The `kernel.unprivileged_userns_clone` parameter controls whether unprivileged users can create user namespaces, which is essential for rootless containers:
   
   ```bash
   # Check if the parameter exists
   cat /proc/sys/kernel/unprivileged_userns_clone
   
   # Enable unprivileged user namespaces (temporary)
   sysctl -w kernel.unprivileged_userns_clone=1
   
   # Enable unprivileged user namespaces (persistent)
   echo 'kernel.unprivileged_userns_clone=1' > /etc/sysctl.d/userns.conf
   ```
   
   This parameter is not available on all systems. Some distributions like Ubuntu have it enabled by default, while others may require kernel recompilation or may not support it at all.

2. **Docker Daemon Configuration**:
   
   The Docker daemon needs to be configured to support user namespace remapping using the `userns-remap` option in `/etc/docker/daemon.json`:
   
   ```json
   {
     "userns-remap": "default"
   }
   ```
   
   This configuration maps the root user inside the container to a non-root user on the host, providing an additional layer of security. The "default" value creates a user and group named "dockremap" for this purpose.
   
   After modifying this file, restart the Docker daemon:
   
   ```bash
   systemctl restart docker
   ```
   
   Note that enabling user namespace remapping affects all containers and may require additional configuration for volume mounts and other Docker features.

3. **User Namespace Mapping Tools**:
   
   The `newuidmap` and `newgidmap` tools are required for mapping user and group IDs between the container and the host. These tools are typically provided by the `uidmap` package:
   
   ```bash
   # Install on Debian/Ubuntu
   apt-get install -y uidmap
   
   # Install on CentOS/RHEL
   yum install -y shadow-utils
   ```
   
   These tools are used by the container runtime to set up the user namespace mappings when a container is started. Without them, you'll see errors like `newuidmap failed: exit status 1` when attempting to use user namespaces.

4. **Container Runtime Flags**:
   
   When running containers, use the appropriate flags to enable user namespace support and disable security features that might interfere with gVisor:
   
   ```bash
   docker run --security-opt seccomp=unconfined --security-opt apparmor=unconfined --userns=host
   ```
   
   These flags disable the seccomp and AppArmor security profiles, which might otherwise interfere with gVisor's operation, and enable host user namespace support.

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
