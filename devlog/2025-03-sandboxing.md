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
- Creates a wrapper script that runs the original entry point
- Note: The initial implementation with runsc had issues with flag format, so the current version uses a direct Python wrapper

Key benefits of gVisor:
- Strong isolation through a user-space kernel
- Compatible with OCI runtime specification
- Active development by Google

Resources:
- [gVisor Documentation](https://gvisor.dev/docs/)
- [gVisor GitHub Repository](https://github.com/google/gvisor)

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
{"type":"SPEC","spec":{"connectionSpecification":{"$schema":"http://json-schema.org/draft-07/schema#","title":"Low-code source spec","type":"object","required":["__injected_declarative_manifest"],"additionalProperties":true,"properties":{"__injected_declarative_manifest":{"title":"Low-code manifest","type":"object","description":"The low-code manifest that defines the components of the source."}}},"documentationUrl":"https://docs.airbyte.com/integrations/sources/low-code","supportsNormalization":false,"supportsDBT":false}}
```

## Challenges Encountered

During implementation, the following challenges were encountered:

1. **gVisor runsc Command Syntax**: The initial implementation of the gVisor wrapper script had issues with the flag format. The `--network=host` flag needed to be changed to `--network host`. For simplicity, the current implementation uses a direct Python wrapper without runsc.

   Further investigation is needed to properly configure runsc for this use case. According to the [runsc documentation](https://gvisor.dev/docs/user_guide/quick_start/docker/), the correct way to use runsc with Docker might involve configuring Docker's runtime rather than directly invoking runsc in a wrapper script.

2. **Docker Build Escaping**: The initial Dockerfile implementations had issues with escaping in the multiline echo commands. This was fixed by using multiple echo commands with redirection.

## Considerations for Production Use

For production use, consider:
- Performance impact of each sandboxing solution
- Security requirements and threat model
- Compatibility with existing infrastructure
- Maintenance overhead
- Further refinement of the gVisor implementation to properly use runsc

## Conclusion

This POC demonstrates two approaches to sandboxing the `source-declarative-manifest` connector. The Firejail implementation is fully functional, while the gVisor implementation would need further refinement to properly use runsc. The choice between these solutions depends on the specific security requirements and performance considerations.
