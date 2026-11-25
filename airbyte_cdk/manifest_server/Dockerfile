# Dockerfile for the Airbyte Manifest Server.
#
# This Dockerfile should be built from the root of the repository.
#
# Example:
# docker build -f airbyte_cdk/manifest_server/Dockerfile -t airbyte/manifest-server .

FROM python:3.13.18-slim-bookworm@sha256:f0fa218d5b06f9b9235024fa061cde2cb141f726de52134471053d2d9b5324a3 AS builder

# Install git (needed only at build time for dynamic versioning) and Poetry >=2.3.3 (CVE-2026-34591)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir poetry==2.5.1

# Create an isolated virtualenv for runtime dependencies only (excluding poetry and dev dependencies)
RUN python -m venv --without-pip /opt/venv

ENV VIRTUAL_ENV=/opt/venv \
    POETRY_NO_INTERACTION=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

WORKDIR /app

# Copy lockfiles and install only main + manifest-server runtime dependencies
COPY pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=$POETRY_CACHE_DIR \
    poetry install --only main --extras manifest-server --no-root

# Copy the project source code (needed for dynamic versioning) and install the package into /opt/venv
COPY . /app
RUN if ! git rev-parse --git-dir >/dev/null 2>&1; then \
        export POETRY_DYNAMIC_VERSIONING_BYPASS=0.0.0dev0; \
    fi && \
    pip --python /opt/venv/bin/python install --no-deps --no-cache-dir .
RUN /opt/venv/bin/python -m compileall -q -j 0 -x '/tests?/' /opt/venv/lib/python3.13/site-packages \
    || echo "WARNING: some files failed to compile (see errors above); continuing without their bytecode"
RUN /opt/venv/bin/python -c "import airbyte_cdk.manifest_server.app"

FROM python:3.13.18-slim-bookworm@sha256:f0fa218d5b06f9b9235024fa061cde2cb141f726de52134471053d2d9b5324a3 AS runtime

# Apply latest Bookworm OS security patches (e.g. libgnutls30, libpcre2-8-0, apt, gpgv, libcap2)
# and remove system pip/ensurepip (not needed at runtime; eliminates vendored package CVEs)
RUN apt-get update && \
    apt-get upgrade -y && \
    rm -rf /var/lib/apt/lists/* && \
    rm -rf /usr/local/lib/python3.13/site-packages/pip* \
           /usr/local/bin/pip* \
           /usr/local/lib/python3.13/ensurepip

# Copy only the runtime virtualenv (root-owned, read-only to the non-root airbyte user)
COPY --from=builder /opt/venv /opt/venv

WORKDIR /app

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1

# Create a non-root user and group
RUN groupadd --gid 1000 airbyte && \
    useradd --uid 1000 --gid airbyte --shell /bin/bash --create-home airbyte

# Run app as non-root user
USER airbyte:airbyte

EXPOSE 8080

CMD ["uvicorn", "airbyte_cdk.manifest_server.app:app", "--host", "0.0.0.0", "--port", "8080"]
