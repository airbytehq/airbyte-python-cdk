#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


class ReadException(Exception):
    """
    Raise when there is an error reading data from an API Source
    """


class RecordNotFoundException(ReadException):
    """Raised when a requested record is not found (e.g., 404 response)."""
