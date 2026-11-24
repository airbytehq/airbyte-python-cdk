#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import errno
import os
import stat
import uuid
from pathlib import Path
from typing import Iterable

from airbyte_cdk.sources.utils.files_directory import get_files_directory

from .file_writer import FileWriter


class LocalFileSystemFileWriter(FileWriter):
    def write(self, file_path: Path, chunks: Iterable[bytes]) -> int:
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

            return self._stage_file(directory_fd, directory_flags, relative_path.name, chunks)
        finally:
            os.close(directory_fd)

    @staticmethod
    def _stage_file(
        directory_fd: int, directory_flags: int, filename: str, chunks: Iterable[bytes]
    ) -> int:
        temporary_dir = f".airbyte-upload-{uuid.uuid4().hex}"
        os.mkdir(temporary_dir, 0o700, dir_fd=directory_fd)
        try:
            temporary_fd = os.open(temporary_dir, directory_flags, dir_fd=directory_fd)
            try:
                file_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
                file_fd = os.open("attachment", file_flags, 0o666, dir_fd=temporary_fd)
                try:
                    with os.fdopen(file_fd, "wb") as staged_file:
                        size = sum(staged_file.write(chunk) for chunk in chunks)
                    try:
                        destination = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        pass
                    else:
                        if stat.S_ISLNK(destination.st_mode):
                            raise ValueError(
                                "File path must not contain symlinks under the files directory"
                            )
                    os.replace(
                        "attachment", filename, src_dir_fd=temporary_fd, dst_dir_fd=directory_fd
                    )
                except BaseException:
                    os.unlink("attachment", dir_fd=temporary_fd)
                    raise
                return size
            finally:
                os.close(temporary_fd)
        finally:
            os.rmdir(temporary_dir, dir_fd=directory_fd)
