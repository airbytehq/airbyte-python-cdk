import os

if os.getenv("DD_ENABLED", "false").lower() == "true":
    # Auto-instrumentation should be imported as early as possible.
    import ddtrace.auto  # noqa: F401

from fastapi import FastAPI

from airbyte_cdk.utils import is_cloud_environment
from airbyte_cdk.utils.outbound_request_filter import (
    cloud_outbound_request_policy,
    init_internal_request_filter,
)

from .helpers.security_events import log_egress_denied
from .routers import capabilities, health, manifest


def _configure_outbound_request_filter() -> None:
    if is_cloud_environment():
        init_internal_request_filter(
            cloud_outbound_request_policy(include_airbyte_service_names=True),
            deny_observer=log_egress_denied,
        )


_configure_outbound_request_filter()

app = FastAPI(
    title="Manifest Server",
    description="A service for running low-code Airbyte connectors",
    version="0.2.0",
    contact={
        "name": "Airbyte",
        "url": "https://airbyte.com",
    },
)

app.include_router(health.router)
app.include_router(capabilities.router)
app.include_router(manifest.router, prefix="/v1")
