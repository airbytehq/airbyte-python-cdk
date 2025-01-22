# This Dockerfile is used to build `airbyte/source-declarative-manifest` image that in turn is used
# 1. to build Manifest-only connectors themselves
# 2. to run manifest (Builder) connectors published into a particular user's workspace in Airbyte
#
# A new version of source-declarative-manifest is built for every new Airbyte CDK release, and their versions are kept in sync.
#

FROM docker.io/airbyte/python-connector-base:3.0.0@sha256:1a0845ff2b30eafa793c6eee4e8f4283c2e52e1bbd44eed6cb9e9abd5d34d844

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
    && cp /usr/local/lib/python3.10/site-packages/airbyte_cdk/cli/source_declarative_manifest/_run.py source_declarative_manifest/run.py \
    && cp /usr/local/lib/python3.10/site-packages/airbyte_cdk/cli/source_declarative_manifest/spec.json source_declarative_manifest/

# Remove unnecessary build files
RUN rm -rf dist/ pyproject.toml poetry.lock README.md

# Set ownership of /airbyte to the non-root airbyte user and group (1000:1000)
RUN chown -R 1000:1000 /airbyte

# Set the entrypoint
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
USER airbyte
