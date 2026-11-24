#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable


class FileWriter(ABC):
    """
    Base File writer class
    """

    @abstractmethod
    def write(self, file_path: Path, chunks: Iterable[bytes]) -> int:
        """
        Writes the file to the specified location
        """
        ...
