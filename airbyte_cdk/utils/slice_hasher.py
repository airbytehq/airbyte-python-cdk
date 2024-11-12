import json
from typing import Any, Mapping, Optional


class SliceEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if hasattr(obj, "__json_serializable__"):
            return obj.__json_serializable__()

        # Let the base class default method raise the TypeError
        return super().default(obj)


class SliceHasher:
    @classmethod
    def hash(cls, stream_name: str, stream_slice: Optional[Mapping[str, Any]] = None) -> int:
        if stream_slice:
            # Convert the slice to a string so that it can be hashed
            s = json.dumps(stream_slice, sort_keys=True, cls=SliceEncoder)
            return hash((stream_name, s))
        else:
            return hash(stream_name)
