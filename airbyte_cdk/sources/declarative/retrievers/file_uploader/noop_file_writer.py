#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from pathlib import Path

from airbyte_cdk.sources.declarative.retrievers.file_uploader import BaseFileWriter


class NoopFileWriter(BaseFileWriter):

    def write(self, file_path: Path, content: bytes) -> int:
        """
        Noop file writer
        """
        return 0