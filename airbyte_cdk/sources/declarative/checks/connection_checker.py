#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    import logging
    from collections.abc import Mapping

    from airbyte_cdk import AbstractSource


class ConnectionChecker(ABC):
    """Abstract base class for checking a connection"""

    @abstractmethod
    def check_connection(
        self, source: AbstractSource, logger: logging.Logger, config: Mapping[str, Any]
    ) -> tuple[bool, Any]:
        """Tests if the input configuration can be used to successfully connect to the integration e.g: if a provided Stripe API token can be used to connect
        to the Stripe API.

        :param source: source
        :param logger: source logger
        :param config: The user-provided configuration as specified by the source's spec.
          This usually contains information required to check connection e.g. tokens, secrets and keys etc.
        :return: A tuple of (boolean, error). If boolean is true, then the connection check is successful
          and we can connect to the underlying data source using the provided configuration.
          Otherwise, the input config cannot be used to connect to the underlying data source,
          and the "error" object should describe what went wrong.
          The error object will be cast to string to display the problem to the user.
        """
        pass
