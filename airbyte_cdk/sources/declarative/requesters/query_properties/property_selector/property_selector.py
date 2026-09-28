# Copyright (c) 2025 Airbyte, Inc., all rights reserved.

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Set


@dataclass
class PropertySelector(ABC):
    """
    Describes the interface for selecting and transforming properties from a configured stream's schema
    to determine which properties should be queried from the API.
    """

    @abstractmethod
    def select(self) -> Optional[Set[str]]:
        """
        Selects and returns the set of properties that should be queried from the API based on the
        configured stream's schema and any applicable transformations.

        Returns:
            Set[str]: The set of property names to query
        """
        pass
