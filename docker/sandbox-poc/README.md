# Sandbox POC Dockerfiles

This directory contains Dockerfiles for proof-of-concept (POC) implementations of sandboxing solutions for the source-declarative-manifest connector.

## Firejail

The `Dockerfile.firejail` adds [Firejail](https://firejail.wordpress.com/) to the source-declarative-manifest image. Firejail is a SUID sandbox program that restricts the running environment of untrusted applications using Linux namespaces and seccomp-bpf.

To build the image:
```
docker build -f Dockerfile.firejail -t airbyte/source-declarative-manifest-firejail .
```

## gVisor

The `Dockerfile.gvisor` adds [gVisor](https://gvisor.dev/) (via runsc) to the source-declarative-manifest image. gVisor is a user-space kernel, written in Go, that implements a substantial portion of the Linux system call interface. It provides an additional layer of isolation between running applications and the host operating system.

To build the image:
```
docker build -f Dockerfile.gvisor -t airbyte/source-declarative-manifest-gvisor .
```

## Usage

Both images wrap the original entry point of the source-declarative-manifest connector with their respective sandboxing solution. The wrapped entry point handles all the same command-line arguments as the original entry point.
