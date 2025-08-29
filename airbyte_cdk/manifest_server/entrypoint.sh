#!/usr/bin/env sh
set -eu

# If Datadog is enabled, prefix command with ddtrace-run
if [ "${DD_ENABLED:-false}" = "true" ]; then
  echo "Datadog tracing enabled"
  set -- ddtrace-run "$@"
fi

exec "$@"