#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable

from .file_writer import FileWriter


class LocalFileSystemFileWriter(FileWriter):
    def write(self, file_path: Path, chunks: Iterable[bytes]) -> int:
        """
        Writes the file to the specified location
        """
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(dir=file_path.parent) as temporary_directory:
            temporary_path = Path(temporary_directory) / "attachment"
            with temporary_path.open("wb") as staged_file:
                size = sum(staged_file.write(chunk) for chunk in chunks)
            temporary_path.replace(file_path)
            return size
