#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import abstractmethod
from typing import Any, List, Mapping, Tuple

from airbyte_cdk.connector_builder.models import (
    LogMessage as ConnectorBuilderLogMessage,
)
from airbyte_cdk.sources.abstract_source import AbstractSource
from airbyte_cdk.sources.declarative.checks.connection_checker import ConnectionChecker


class DeclarativeSource(AbstractSource):
    """
    Base class for declarative Source. Concrete sources need to define the connection_checker to use
    """

    @property
    @abstractmethod
    def connection_checker(self) -> ConnectionChecker:
        """Returns the ConnectionChecker to use for the `check` operation"""

    def check_connection(
        self, logger: logging.Logger, config: Mapping[str, Any]
    ) -> Tuple[bool, Any]:
        """
        :param logger: The source logger
        :param config: The user-provided configuration as specified by the source's spec.
          This usually contains information required to check connection e.g. tokens, secrets and keys etc.
        :return: A tuple of (boolean, error). If boolean is true, then the connection check is successful
          and we can connect to the underlying data source using the provided configuration.
          Otherwise, the input config cannot be used to connect to the underlying data source,
          and the "error" object should describe what went wrong.
          The error object will be cast to string to display the problem to the user.
        """
        return self.connection_checker.check_connection(self, logger, config)

    def deprecation_warnings(self) -> List[ConnectorBuilderLogMessage]:
        """
        Returns a list of deprecation warnings for the source.
        """
        return []
