# Builds as `airbyte/source-declarative-manifest`
# Usage:
#  docker build -t airbyte/source-declarative-manifest .
#  docker run airbyte/source-declarative-manifest --help
#  docker run airbyte/source-declarative-manifest spec
FROM docker.io/airbyte/python-connector-base:2.0.0@sha256:c44839ba84406116e8ba68722a0f30e8f6e7056c726f447681bb9e9ece8bd916

WORKDIR /airbyte-cdk
# Copy source code into image
COPY ./airbyte_cdk /airbyte-cdk/airbyte_cdk
COPY ./pyproject.toml /airbyte-cdk/
COPY ./poetry.lock /airbyte-cdk/
COPY ./README.md /airbyte-cdk/

RUN poetry install --only main
ENTRYPOINT [ "poetry run source-declarative-manifest" ]
