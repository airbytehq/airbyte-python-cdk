from dataclasses import dataclass
from typing import Protocol, Type


class ConnectorInterface(Protocol):
    """Protocol for Airbyte connectors."""

    @classmethod
    def launch(cls, args: list[str] | None): ...


@dataclass
class PythonWrapper:
    """Wrapper for Python source and destination connectors."""

    connector_class: Type["ConnectorInterface"]
