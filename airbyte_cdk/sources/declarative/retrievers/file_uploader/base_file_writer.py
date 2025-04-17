#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from pathlib import Path

from abc import ABC, abstractmethod

class BaseFileWriter(ABC):
    """
    Base File writer class
    """

    @abstractmethod
    def write(self, file_path: Path, content: bytes) -> int:
        """
        Writes the file to the specified location
        """
        ...