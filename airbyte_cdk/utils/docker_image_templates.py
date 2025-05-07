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
        "*,"  # Ignore everything not explicitly allowed below
        "build/",
        "!build/distributions/*.tar",
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

PYTHON_CONNECTOR_DOCKERFILE_TEMPLATE = r"""
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

# Build arguments
ARG BASE_IMAGE

# Base image - using Amazon Corretto (Amazon's distribution of OpenJDK)
FROM ${BASE_IMAGE}
ARG CONNECTOR_KEBAB_NAME

# Set permissions for downloaded files
RUN chmod +x /airbyte/base.sh /airbyte/javabase.sh && \
    chown airbyte:airbyte /airbyte/base.sh /airbyte/javabase.sh /airbyte/dd-java-agent.jar

ENV AIRBYTE_ENTRYPOINT="/airbyte/base.sh"
ENV APPLICATION="${CONNECTOR_KEBAB_NAME}"

# Add the connector TAR file and extract it
COPY ./build/distributions/${CONNECTOR_KEBAB_NAME}.tar /tmp/${CONNECTOR_KEBAB_NAME}.tar
RUN tar xf /tmp/${CONNECTOR_KEBAB_NAME}.tar --strip-components=1 -C /airbyte && \
    rm -rf /tmp/${CONNECTOR_KEBAB_NAME}.tar && \
    chown -R airbyte:airbyte /airbyte

# Set the non-root user
USER airbyte

# Set entrypoint
ENTRYPOINT ["/airbyte/base.sh"]
"""
