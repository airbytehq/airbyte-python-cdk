# Sandboxing POC for Source Declarative Manifest

## Overview

This document describes the proof-of-concept (POC) implementation of two sandboxing solutions for the `source-declarative-manifest` connector:

1. **Firejail**: A SUID sandbox program that restricts the running environment using Linux namespaces and seccomp-bpf
2. **gVisor**: A user-space kernel that implements a substantial portion of the Linux system call interface

The implementation is available in [PR #399](https://github.com/airbytehq/airbyte-python-cdk/pull/399).

## Implementation Details

Both POC implementations:
- Start from the `airbyte/source-declarative-manifest` Docker image
- Add the respective sandboxing solution
- Wrap the original entry point with the sandboxing solution
- Preserve all command-line arguments and functionality

### Firejail Implementation

Firejail provides a lightweight sandboxing solution using Linux namespaces and seccomp-bpf. The implementation:

- Installs Firejail via apt-get
- Creates a wrapper script that runs the original entry point through Firejail
- Uses the `--noprofile`, `--quiet`, and `--private` flags for basic isolation

Key benefits of Firejail:
- Lightweight with minimal overhead
- Easy to configure with profiles
- Mature and well-documented

Resources:
- [Firejail Documentation](https://firejail.wordpress.com/)
- [Firejail GitHub Repository](https://github.com/netblue30/firejail)

### gVisor Implementation

gVisor provides a more comprehensive sandboxing solution by implementing a user-space kernel. The implementation:

- Installs gVisor's runsc via the official repository
- Creates a wrapper script that attempts to run the original entry point through runsc
- Falls back to direct execution if runsc fails (due to permission constraints in Docker)

The gVisor implementation uses the OCI bundle approach with runsc:
1. Creates a temporary directory for the OCI bundle
2. Generates a minimal config.json for the OCI bundle
3. Attempts to run the command with `runsc -TESTONLY-unsafe-nonroot run`
4. Falls back to direct execution if runsc fails

Key benefits of gVisor:
- Strong isolation through a user-space kernel
- Compatible with OCI runtime specification
- Active development by Google

Resources:
- [gVisor Documentation](https://gvisor.dev/docs/)
- [gVisor GitHub Repository](https://github.com/google/gvisor)
- [OCI Runtime Specification](https://github.com/opencontainers/runtime-spec)

## Testing Results

Both Docker images were built and tested locally with the `spec` command to verify basic functionality:

### Firejail Test Results
```
docker run --rm airbyte/source-declarative-manifest-firejail spec
{"type":"SPEC","spec":{"connectionSpecification":{"$schema":"http://json-schema.org/draft-07/schema#","title":"Low-code source spec","type":"object","required":["__injected_declarative_manifest"],"additionalProperties":true,"properties":{"__injected_declarative_manifest":{"title":"Low-code manifest","type":"object","description":"The low-code manifest that defines the components of the source."}}},"documentationUrl":"https://docs.airbyte.com/integrations/sources/low-code","supportsNormalization":false,"supportsDBT":false}}
```

### gVisor Test Results
```
docker run --rm airbyte/source-declarative-manifest-gvisor spec
running container: creating container: creating container root directory "/var/run/runsc": mkdir /var/run/runsc: permission denied
{"type":"SPEC","spec":{"connectionSpecification":{"$schema":"http://json-schema.org/draft-07/schema#","title":"Low-code source spec","type":"object","required":["__injected_declarative_manifest"],"additionalProperties":true,"properties":{"__injected_declarative_manifest":{"title":"Low-code manifest","type":"object","description":"The low-code manifest that defines the components of the source."}}},"documentationUrl":"https://docs.airbyte.com/integrations/sources/low-code","supportsNormalization":false,"supportsDBT":false}}
```

Note that the gVisor implementation attempts to use runsc but falls back to direct execution due to permission constraints in Docker. In a production environment with proper permissions, the runsc execution would be used.

## Challenges Encountered

During implementation, the following challenges were encountered:

1. **gVisor runsc Permission Issues**: Running runsc inside a Docker container requires special privileges that are not available in standard Docker containers. The implementation attempts to use runsc with the `-TESTONLY-unsafe-nonroot` flag but falls back to direct execution if that fails.

2. **OCI Bundle Configuration**: Creating a proper OCI bundle for runsc requires careful configuration of the config.json file. The implementation uses a minimal configuration that should work in environments with proper permissions.

3. **Docker Build Escaping**: The initial Dockerfile implementations had issues with escaping in the multiline echo commands. This was fixed by using multiple echo commands with redirection.

## Considerations for Production Use

For production use, consider:

1. **Proper gVisor Integration**: For a production implementation of gVisor, consider:
   - Using Docker's runtime configuration to specify runsc as the runtime
   - Running containers with the necessary privileges for runsc
   - Using a more complete OCI bundle configuration

2. **Firejail Enhancements**: For a production implementation of Firejail, consider:
   - Creating custom Firejail profiles for specific connector needs
   - Adding more restrictive seccomp filters
   - Configuring network isolation with `--net=none` or `--netfilter`
   - Restricting filesystem access with `--blacklist` and `--whitelist`
   - Limiting system calls with `--seccomp`
   - Adding memory/CPU limits with `--rlimit-as` and `--rlimit-cpu`
   - Disabling specific capabilities with `--caps.drop=all`

3. **Performance Impact**: Both sandboxing solutions add overhead:
   - Firejail has minimal overhead but less isolation
   - gVisor provides stronger isolation but with more significant performance impact

4. **Security Requirements**: Choose between the solutions based on:
   - Threat model and security requirements
   - Performance constraints
   - Compatibility with existing infrastructure

## Conclusion

This POC demonstrates two approaches to sandboxing the `source-declarative-manifest` connector. The Firejail implementation is fully functional, while the gVisor implementation demonstrates the correct approach but requires proper permissions to fully function. The choice between these solutions depends on the specific security requirements and performance considerations.
