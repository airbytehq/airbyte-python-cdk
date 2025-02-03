#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.sources.file_based.config.abstract_file_based_spec import AbstractFileBasedSpec


def use_file_transfer(parsed_config: AbstractFileBasedSpec) -> bool:
    return (
        hasattr(parsed_config.delivery_method, "delivery_type")
        and parsed_config.delivery_method.delivery_type == "use_file_transfer"
    )


def preserve_directory_structure(parsed_config: AbstractFileBasedSpec) -> bool:
    """
    Determines whether to preserve directory structure during file transfer.

    When enabled, files maintain their subdirectory paths in the destination.
    When disabled, files are flattened to the root of the destination.

    Args:
        parsed_config: The parsed configuration containing delivery method settings

    Returns:
        True if directory structure should be preserved (default), False otherwise
    """
    if (
        use_file_transfer(parsed_config)
        and hasattr(parsed_config.delivery_method, "preserve_directory_structure")
        and parsed_config.delivery_method.preserve_directory_structure is not None
    ):
        return parsed_config.delivery_method.preserve_directory_structure
    return True


def use_permissions_transfer(parsed_config: AbstractFileBasedSpec) -> bool:
    return (
        hasattr(parsed_config.delivery_method, "delivery_type")
        and parsed_config.delivery_method.delivery_type == "use_permissions_transfer"
    )


def include_identities_stream(parsed_config: AbstractFileBasedSpec) -> bool:
    if (
        use_permissions_transfer(parsed_config)
        and hasattr(parsed_config.delivery_method, "include_identities_stream")
        and parsed_config.delivery_method.include_identities_stream is not None
    ):
        return parsed_config.delivery_method.include_identities_stream
    return False
