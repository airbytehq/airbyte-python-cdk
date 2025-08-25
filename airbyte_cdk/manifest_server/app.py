from fastapi import FastAPI

from .routers import capabilities, health, manifest

app = FastAPI(
    title="Manifest Server",
    description="A service for running low-code Airbyte connectors",
    version="0.1.0",
    contact={
        "name": "Airbyte",
        "url": "https://airbyte.com",
    },
)

app.include_router(health.router)
app.include_router(capabilities.router)
app.include_router(manifest.router, prefix="/v1")
