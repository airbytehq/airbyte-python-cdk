#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Iterable

    from airbyte_cdk.sources.streams.concurrent.partitions.partition import Partition


class PartitionGenerator(ABC):
    @abstractmethod
    def generate(self) -> Iterable[Partition]:
        """Generates partitions for a given sync mode.
        :return: An iterable of partitions
        """
        pass
