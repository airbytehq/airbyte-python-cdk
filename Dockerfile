FROM docker.io/airbyte/python-connector-base:2.0.0@sha256:c44839ba84406116e8ba68722a0f30e8f6e7056c726f447681bb9e9ece8bd916

WORKDIR /airbyte/integration_code

# Copy project files needed for build
COPY pyproject.toml poetry.lock README.md ./

# Install dependencies - ignore keyring warnings
RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi || true

# Copy source code
COPY airbyte_cdk ./airbyte_cdk

# Build and install the package
RUN poetry build && pip install dist/*.whl

ENTRYPOINT ["poetry", "run", "source-declarative-manifest"]
