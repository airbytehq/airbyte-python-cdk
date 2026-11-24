#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from pathlib import Path
from typing import Iterable

from .file_writer import FileWriter


class NoopFileWriter(FileWriter):
    NOOP_FILE_SIZE = -1

    def write(self, file_path: Path, chunks: Iterable[bytes]) -> int:
        """
        Noop file writer
        """
        for _ in chunks:
            pass
        return self.NOOP_FILE_SIZE
