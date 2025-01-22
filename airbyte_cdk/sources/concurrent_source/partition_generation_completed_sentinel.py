#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.sources.streams.concurrent.abstract_stream import AbstractStream


class PartitionGenerationCompletedSentinel:  # noqa: PLW1641
    """
    A sentinel object indicating all partitions for a stream were produced.
    Includes a pointer to the stream that was processed.
    """

    def __init__(self, stream: AbstractStream):  # noqa: ANN204
        """
        :param stream: The stream that was processed
        """
        self.stream = stream

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PartitionGenerationCompletedSentinel):
            return self.stream == other.stream
        return False
