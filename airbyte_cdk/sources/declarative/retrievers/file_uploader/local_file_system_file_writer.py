#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import errno
import os
from pathlib import Path

from airbyte_cdk.sources.utils.files_directory import get_files_directory

from .file_writer import FileWriter


class LocalFileSystemFileWriter(FileWriter):
    def write(self, file_path: Path, content: bytes) -> int:
        """
        Writes the file without following symlinks outside the selected files directory.
        """
        configured_files_directory = Path(get_files_directory())
        try:
            relative_path = file_path.relative_to(configured_files_directory)
        except ValueError as error:
            raise ValueError("File path must remain within the files directory") from error

        if not relative_path.parts or ".." in relative_path.parts:
            raise ValueError("File path must remain within the files directory")

        configured_files_directory.mkdir(parents=True, exist_ok=True)
        files_directory = configured_files_directory.resolve(strict=True)
        directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        directory_fd = os.open(files_directory, directory_flags)
        try:
            for directory_name in relative_path.parts[:-1]:
                try:
                    os.mkdir(directory_name, dir_fd=directory_fd)
                except FileExistsError:
                    pass

                try:
                    next_directory_fd = os.open(
                        directory_name,
                        directory_flags,
                        dir_fd=directory_fd,
                    )
                except OSError as error:
                    if error.errno in (errno.ELOOP, errno.ENOTDIR):
                        raise ValueError(
                            "File path must not contain symlinks under the files directory"
                        ) from error
                    raise

                os.close(directory_fd)
                directory_fd = next_directory_fd

            file_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW
            try:
                file_descriptor = os.open(
                    relative_path.name,
                    file_flags,
                    0o666,
                    dir_fd=directory_fd,
                )
            except OSError as error:
                if error.errno == errno.ELOOP:
                    raise ValueError(
                        "File path must not contain symlinks under the files directory"
                    ) from error
                raise

            with os.fdopen(file_descriptor, "wb") as file:
                bytes_written = file.write(content)
        finally:
            os.close(directory_fd)

        return bytes_written
