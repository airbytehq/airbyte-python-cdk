import hashlib
from dataclasses import asdict
from typing import Any, Dict, List

from fastapi import APIRouter, Depends

from airbyte_cdk.models import AirbyteStateMessageSerializer
from airbyte_cdk.sources.declarative.parsers.custom_code_compiler import (
    INJECTED_COMPONENTS_PY,
    INJECTED_COMPONENTS_PY_CHECKSUMS,
)

from ..api_models import (
    FullResolveRequest,
    ManifestResponse,
    ResolveRequest,
    StreamRead,
    StreamTestReadRequest,
)
from ..auth import verify_jwt_token
from ..manifest_runner.runner import ManifestRunner
from ..manifest_runner.utils import build_catalog, build_source

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
    source = build_source(request.manifest.model_dump(), config_dict)
    catalog = build_catalog(request.stream_name)
    state = [AirbyteStateMessageSerializer.load(state) for state in request.state]

    if request.custom_components_code:
        config_dict[INJECTED_COMPONENTS_PY] = request.custom_components_code
        config_dict[INJECTED_COMPONENTS_PY_CHECKSUMS] = {
            "md5": hashlib.md5(request.custom_components_code.encode()).hexdigest()
        }

    runner = ManifestRunner(source)
    cdk_result = runner.test_read(
        config_dict,
        catalog,
        state,
        request.record_limit,
        request.page_limit,
        request.slice_limit,
    )
    return StreamRead.model_validate(asdict(cdk_result))


@router.post("/resolve", operation_id="resolve")
def resolve(request: ResolveRequest) -> ManifestResponse:
    """Resolve a manifest to its final configuration."""
    source = build_source(request.manifest.model_dump(), {})
    return ManifestResponse(manifest=source.resolved_manifest)


@router.post("/full_resolve", operation_id="fullResolve")
def full_resolve(request: FullResolveRequest) -> ManifestResponse:
    """
    Fully resolve a manifest including dynamic streams.

    Generates dynamic streams up to the specified limit and includes
    them in the resolved manifest.
    """
    source = build_source(request.manifest.model_dump(), request.config.model_dump())
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
    return ManifestResponse(manifest=manifest)
