#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


class CircularReferenceException(Exception):
    """
    Raised when a circular reference is detected in a manifest.
    """

    def __init__(self, reference: str) -> None:
        super().__init__(f"Circular reference found: {reference}")


class UndefinedReferenceException(Exception):
    """
    Raised when refering to an undefined reference.
    """

    def __init__(self, path: str, reference: str) -> None:
        super().__init__(f"Undefined reference {reference} from {path}")


class ManifestNormalizationException(Exception):
    """
    Raised when a circular reference is detected in a manifest.
    """

    def __init__(self, message: str) -> None:
        super().__init__(f"Failed to deduplicate manifest: {message}")


class ManifestMigrationException(Exception):
    """
    Raised when a migration error occurs in the manifest.
    """

    def __init__(self, message: str) -> None:
        super().__init__(f"Failed to migrate the manifest: {message}")
