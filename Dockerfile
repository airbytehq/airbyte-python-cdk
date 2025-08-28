# This Dockerfile is used to build `airbyte/source-declarative-manifest` image that in turn is used
# 1. to build Manifest-only connectors themselves
# 2. to run manifest (Builder) connectors published into a particular user's workspace in Airbyte
#
# A new version of source-declarative-manifest is built for every new Airbyte CDK release, and their versions are kept in sync.
#
# Most of the below is in common with the Python Connector Base Image
# - https://github.com/airbytehq/airbyte/blob/master/docker-images/Dockerfile.python-connector-base
#
# We moved off of the Python base image in Aug 2025, in order to decouple the Python version used by
# Python connectors and the Manifest-Only executions. (Python connectors require additional testing
# due to external library dependencies.)

# https://hub.docker.com/_/python/tags?name=3.13.7-slim-bookworm
ARG BASE_IMAGE=docker.io/python:3.13.7-slim-bookworm@sha256:9b8102b7b3a61db24fe58f335b526173e5aeaaf7d13b2fbfb514e20f84f5e386
FROM ${BASE_IMAGE}

# Set the timezone to UTC
RUN ln -snf /usr/share/zoneinfo/Etc/UTC /etc/localtime

# Set-up groups, users, and directories
RUN adduser --uid 1000 --system --group --no-create-home airbyte

# Create the cache airbyte directories and set the right permissions
RUN mkdir --mode 755 /airbyte && \
    mkdir --mode 755 /custom_cache && \
    mkdir /secrets && \
    mkdir /config && \
    mkdir /nonexistent && \
    chown airbyte:airbyte /airbyte && \
    chown -R airbyte:airbyte /custom_cache && \
    chown -R airbyte:airbyte /secrets && \
    chown -R airbyte:airbyte /config && \
    chown -R airbyte:airbyte /nonexistent && \
    chown -R airbyte:airbyte /tmp

ENV POETRY_VIRTUALENVS_CREATE=false
ENV POETRY_VIRTUALENVS_IN_PROJECT=false
ENV POETRY_NO_INTERACTION=1

# Create and set up pip cache directory
# TODO: Remove this block if not needed.
ENV PIP_CACHE_DIR=/pip_cache
RUN mkdir -p ${PIP_CACHE_DIR} && chown -R airbyte:airbyte ${PIP_CACHE_DIR}

# Install pip and poetry
RUN pip install --upgrade \
      pip==24.0 \
      setuptools==78.1.1 \
      poetry==1.8.4

# Install system dependencies
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get dist-upgrade -y && \
    apt-get clean
RUN apt-get install -y socat=1.7.4.4-2

WORKDIR /airbyte/integration_code

# Copy project files needed for build
COPY pyproject.toml poetry.lock README.md ./
COPY dist/*.whl ./dist/

# Install dependencies - ignore keyring warnings
RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi || true

# Build and install the package
RUN pip install dist/*.whl

# Recreate the original structure
RUN mkdir -p source_declarative_manifest \
    && echo 'from source_declarative_manifest.run import run\n\nif __name__ == "__main__":\n    run()' > main.py \
    && touch source_declarative_manifest/__init__.py \
    && cp /usr/local/lib/python3.11/site-packages/airbyte_cdk/cli/source_declarative_manifest/_run.py source_declarative_manifest/run.py \
    && cp /usr/local/lib/python3.11/site-packages/airbyte_cdk/cli/source_declarative_manifest/spec.json source_declarative_manifest/

# Remove unnecessary build files
RUN rm -rf dist/ pyproject.toml poetry.lock README.md

# Set ownership of /airbyte to the non-root airbyte user and group (1000:1000)
RUN chown -R 1000:1000 /airbyte

# Set the entrypoint
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
USER airbyte
