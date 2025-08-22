import os
from typing import Any, Dict

from fastapi import APIRouter

router = APIRouter(
    prefix="/capabilities",
    tags=["capabilities"],
)


@router.get("/", operation_id="getCapabilities")
def get_capabilities() -> Dict[str, Any]:
    """
    Get the capabilities available for the manifest runner service.

    Returns:
        Dict containing the service capabilities including custom code execution support.
    """
    # Read the same environment variable as the connector builder server
    enable_unsafe_code = os.getenv("AIRBYTE_ENABLE_UNSAFE_CODE", "false").lower() == "true"

    return {"customCodeExecution": enable_unsafe_code}
