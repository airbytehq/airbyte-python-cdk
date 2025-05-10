#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from typing import Any, Dict


class ConfigTransformation(ABC):
    """
    Implementations of this class define transformations that can be applied to source configurations.
    """

    @abstractmethod
    def transform(
        self,
        config: Dict[str, Any],
    ) -> None:
        """
        Transform a configuration by adding, deleting, or mutating fields directly from the config reference passed in argument.

        :param config: The user-provided configuration to be transformed
        """
