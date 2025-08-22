import hashlib
from dataclasses import asdict
from typing import Any, Dict, List, Mapping, Optional

import jsonschema
from fastapi import APIRouter, Depends, HTTPException

from airbyte_cdk.manifest_runner.api_models.manifest import (
    CheckRequest,
    CheckResponse,
    DiscoverRequest,
    DiscoverResponse,
)
from airbyte_cdk.models import AirbyteStateMessageSerializer
from airbyte_cdk.sources.declarative.manifest_declarative_source import ManifestDeclarativeSource
from airbyte_cdk.sources.declarative.parsers.custom_code_compiler import (
    INJECTED_COMPONENTS_PY,
    INJECTED_COMPONENTS_PY_CHECKSUMS,
)

from ..api_models import (
    FullResolveRequest,
    Manifest,
    ManifestResponse,
    ResolveRequest,
    StreamRead,
    StreamTestReadRequest,
)
from ..auth import verify_jwt_token
from ..command_processor.processor import ManifestCommandProcessor
from ..command_processor.utils import build_catalog, build_source


def safe_build_source(
    manifest_dict: Mapping[str, Any],
    config_dict: Mapping[str, Any],
    page_limit: Optional[int] = None,
    slice_limit: Optional[int] = None,
) -> ManifestDeclarativeSource:
    """Wrapper around build_source that converts ValidationError to HTTPException."""
    try:
        return build_source(manifest_dict, config_dict, page_limit, slice_limit)
    except jsonschema.exceptions.ValidationError as e:
        raise HTTPException(status_code=400, detail=f"Invalid manifest: {e.message}")


router = APIRouter(
    prefix="/manifest",
    tags=["manifest"],
    dependencies=[Depends(verify_jwt_token)],
)


@router.post("/test_read", operation_id="testRead")
def test_read(request: StreamTestReadRequest) -> StreamRead:
    """
    Test reading from a specific stream in the manifest.
    """
    config_dict = request.config.model_dump()

    source = safe_build_source(
        request.manifest.model_dump(), config_dict, request.page_limit, request.slice_limit
    )
    catalog = build_catalog(request.stream_name)
    state = [AirbyteStateMessageSerializer.load(state) for state in request.state]

    if request.custom_components_code:
        config_dict[INJECTED_COMPONENTS_PY] = request.custom_components_code
        config_dict[INJECTED_COMPONENTS_PY_CHECKSUMS] = {
            "md5": hashlib.md5(request.custom_components_code.encode()).hexdigest()
        }

    runner = ManifestCommandProcessor(source)
    cdk_result = runner.test_read(
        config_dict,
        catalog,
        state,
        request.record_limit,
        request.page_limit,
        request.slice_limit,
    )
    return StreamRead.model_validate(asdict(cdk_result))


@router.post("/check", operation_id="check")
def check(request: CheckRequest) -> CheckResponse:
    """Check configuration against a manifest"""
    source = safe_build_source(request.manifest.model_dump(), request.config.model_dump())
    runner = ManifestCommandProcessor(source)
    success, message = runner.check_connection(request.config.model_dump())
    return CheckResponse(success=success, message=message)


@router.post("/discover", operation_id="discover")
def discover(request: DiscoverRequest) -> DiscoverResponse:
    """Discover streams from a manifest"""
    source = safe_build_source(request.manifest.model_dump(), request.config.model_dump())
    runner = ManifestCommandProcessor(source)
    catalog = runner.discover(request.config.model_dump())
    if catalog is None:
        raise HTTPException(status_code=422, detail="Connector did not return a discovered catalog")
    return DiscoverResponse(catalog=catalog)


@router.post("/resolve", operation_id="resolve")
def resolve(request: ResolveRequest) -> ManifestResponse:
    """Resolve a manifest to its final configuration."""
    source = safe_build_source(request.manifest.model_dump(), {})
    return ManifestResponse(manifest=Manifest(**source.resolved_manifest))


@router.post("/full_resolve", operation_id="fullResolve")
def full_resolve(request: FullResolveRequest) -> ManifestResponse:
    """
    Fully resolve a manifest including dynamic streams.

    Generates dynamic streams up to the specified limit and includes
    them in the resolved manifest.
    """
    source = safe_build_source(request.manifest.model_dump(), request.config.model_dump())
    manifest = {**source.resolved_manifest}
    streams = manifest.get("streams", [])
    for stream in streams:
        stream["dynamic_stream_name"] = None

    mapped_streams: Dict[str, List[Dict[str, Any]]] = {}
    for stream in source.dynamic_streams:
        generated_streams = mapped_streams.setdefault(stream["dynamic_stream_name"], [])

        if len(generated_streams) < request.stream_limit:
            generated_streams += [stream]

    for generated_streams_list in mapped_streams.values():
        streams.extend(generated_streams_list)

    manifest["streams"] = streams
    return ManifestResponse(manifest=Manifest(**manifest))
