# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""A collection of Dockerfile templates for building Airbyte connectors.

The templates are designed to be used with the Airbyte CDK and can be customized
for different connectors and architectures.

These templates are used to generate connector images.
"""

##############################
## GLOBAL DOCKERIGNORE FILE ##
##############################

DOCKERIGNORE_TEMPLATE: str = "\n".join(
    [
        "# This file is auto-generated. Do not edit.",
        # "*,"
        "build/",
        ".venv/",
        "secrets/",
        "!setup.py",
        "!pyproject.toml",
        "!poetry.lock",
        "!poetry.toml",
        "!components.py",
        "!requirements.txt",
        "!README.md",
        "!metadata.yaml",
        "!build_customization.py",
        "!source_*",
        "!destination_*",
    ]
)

###########################
# PYTHON CONNECTOR IMAGE ##
###########################

PYTHON_CONNECTOR_DOCKERFILE_TEMPLATE = """
# syntax=docker/dockerfile:1
# check=skip=all
ARG BASE_IMAGE

FROM ${BASE_IMAGE} AS builder
ARG BASE_IMAGE
ARG CONNECTOR_SNAKE_NAME
ARG CONNECTOR_KEBAB_NAME
ARG EXTRA_PREREQS_SCRIPT=""

WORKDIR /airbyte/integration_code

COPY . ./

# Conditionally copy and execute the extra build script if provided
RUN if [ -n "${EXTRA_PREREQS_SCRIPT}" ]; then \
        cp ${EXTRA_PREREQS_SCRIPT} ./extra_prereqs_script && \
        ./extra_prereqs_script; \
    fi

# TODO: Pre-install uv on the base image to speed up the build.
#       (uv is still faster even with the extra step.)
RUN pip install --no-cache-dir uv
RUN python -m uv pip install --no-cache-dir .

FROM ${BASE_IMAGE}
ARG CONNECTOR_SNAKE_NAME
ARG CONNECTOR_KEBAB_NAME
ARG BASE_IMAGE

WORKDIR /airbyte/integration_code

COPY --from=builder /usr/local /usr/local
COPY --chmod=755 <<EOT /entrypoint.sh
#!/usr/bin/env bash
set -e

${CONNECTOR_KEBAB_NAME} "\$\@"
EOT

ENV AIRBYTE_ENTRYPOINT="/entrypoint.sh"
ENTRYPOINT ["/entrypoint.sh"]
"""

##################################
# MANIFEST-ONLY CONNECTOR IMAGE ##
##################################

MANIFEST_ONLY_DOCKERFILE_TEMPLATE = """
ARG BASE_IMAGE
ARG CONNECTOR_SNAKE_NAME
ARG CONNECTOR_KEBAB_NAME

FROM ${BASE_IMAGE}

WORKDIR /airbyte/integration_code

COPY . ./

ENV AIRBYTE_ENTRYPOINT="python ./main.py"
ENTRYPOINT ["python", "./main.py"]
"""

#########################
# JAVA CONNECTOR IMAGE ##
#########################

JAVA_CONNECTOR_DOCKERFILE_TEMPLATE = """
# Java connector Dockerfile for Airbyte
# This Dockerfile replicates the dagger-based build process for Java connectors

# Build arguments
ARG JDK_VERSION=21-al2023

# Base image - using Amazon Corretto (Amazon's distribution of OpenJDK)
FROM amazoncorretto:${JDK_VERSION}
ARG CONNECTOR_KEBAB_NAME

# Install required packages and set up the non-root user
RUN set -o xtrace && \
    yum install -y shadow-utils tar openssl findutils && \
    yum update -y --security && \
    yum clean all && \
    rm -rf /var/cache/yum && \
    echo "Creating airbyte user and group..." && \
    groupadd --gid 1000 airbyte && \
    useradd --uid 1000 --gid airbyte --shell /bin/bash --create-home airbyte && \
    echo "Creating directories..." && \
    mkdir /secrets && \
    mkdir /config && \
    mkdir --mode 755 /airbyte && \
    mkdir --mode 755 /custom_cache && \
    echo "Setting permissions..." && \
    chown -R airbyte:airbyte /airbyte && \
    chown -R airbyte:airbyte /custom_cache && \
    chown -R airbyte:airbyte /secrets && \
    chown -R airbyte:airbyte /config && \
    chown -R airbyte:airbyte /usr/share/pki/ca-trust-source && \
    chown -R airbyte:airbyte /etc/pki/ca-trust && \
    chown -R airbyte:airbyte /tmp

# Download required scripts
WORKDIR /airbyte
ADD https://raw.githubusercontent.com/airbytehq/airbyte/6d8a3a2bc4f4ca79f10164447a90fdce5c9ad6f9/airbyte-integrations/bases/base/base.sh /airbyte/base.sh
ADD https://raw.githubusercontent.com/airbytehq/airbyte/6d8a3a2bc4f4ca79f10164447a90fdce5c9ad6f9/airbyte-integrations/bases/base-java/javabase.sh /airbyte/javabase.sh
ADD https://dtdg.co/latest-java-tracer /airbyte/dd-java-agent.jar

# Set permissions for downloaded files
RUN chmod +x /airbyte/base.sh /airbyte/javabase.sh && \
    chown airbyte:airbyte /airbyte/base.sh /airbyte/javabase.sh /airbyte/dd-java-agent.jar

# Set environment variables
# These variables tell base.sh what to do:
ENV AIRBYTE_SPEC_CMD="/airbyte/javabase.sh --spec"
ENV AIRBYTE_CHECK_CMD="/airbyte/javabase.sh --check"
ENV AIRBYTE_DISCOVER_CMD="/airbyte/javabase.sh --discover"
ENV AIRBYTE_READ_CMD="/airbyte/javabase.sh --read"
ENV AIRBYTE_WRITE_CMD="/airbyte/javabase.sh --write"

# base.sh will set the classpath for the connector and invoke javabase.sh
# using one of the above commands.
ENV AIRBYTE_ENTRYPOINT="/airbyte/base.sh"
ENV APPLICATION="${CONNECTOR_KEBAB_NAME}"

# Add the connector TAR file and extract it
COPY airbyte-integrations/connectors/${CONNECTOR_KEBAB_NAME}/build/distributions/${CONNECTOR_KEBAB_NAME}.tar /tmp/${CONNECTOR_KEBAB_NAME}.tar
RUN tar xf /tmp/${CONNECTOR_KEBAB_NAME}.tar --strip-components=1 -C /airbyte && \
    rm -rf /tmp/${CONNECTOR_KEBAB_NAME}.tar && \
    chown -R airbyte:airbyte /airbyte

# Set the non-root user
USER airbyte

# Set entrypoint
ENTRYPOINT ["/airbyte/base.sh"]

# Add labels
LABEL io.airbyte.version="0.1.0"
LABEL io.airbyte.name="airbyte/${CONNECTOR_KEBAB_NAME}"
"""
