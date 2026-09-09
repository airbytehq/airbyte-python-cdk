from typing import Dict

from fastapi import APIRouter

router = APIRouter(
    prefix="/health",
    tags=["health"],
)


@router.get("", include_in_schema=False)
@router.get("/")
def health() -> Dict[str, str]:
    return {"status": "ok"}
