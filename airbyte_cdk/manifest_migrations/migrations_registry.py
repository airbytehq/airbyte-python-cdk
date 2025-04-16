import importlib
import inspect
import pkgutil
import re
import sys
from typing import List, Type

import airbyte_cdk.manifest_migrations.migrations as migrations_pkg
from airbyte_cdk.manifest_migrations.manifest_migration import (
    ManifestMigration,
)

# Dynamically import all modules in the migrations package
for _, module_name, is_pkg in pkgutil.iter_modules(migrations_pkg.__path__):
    if not is_pkg:
        importlib.import_module(f"{migrations_pkg.__name__}.{module_name}")


def _migration_order_key(cls: object) -> int:
    """
    Determines the migration order key for a given migration class based on its module name.

    The function expects the module name to end with a double underscore followed by an integer (e.g., '__0').
    This integer is extracted and returned as the migration order key.

    Args:
        cls (object): The migration class whose module name encodes the migration order.

    Returns:
        int: The migration order extracted from the module name.

    Raises:
        ValueError: If the module name does not contain the expected order suffix.
    """
    # Extract the migration order from the module name, e.g., http_requester_url_base_to_url_v6_45_2__0
    # The order is the integer after the double underscore at the end of the module name
    module_name = cls.__module__.split(".")[-1]
    match = re.search(r"__(\d+)$", module_name)
    if match:
        return int(match.group(1))
    else:
        message = f"Migration `{cls.__module__}` doesn't have the `order` in the module name: {module_name}. Did you miss to add `__<order>` to the module name?"
        raise ValueError(message)


def _discover_migrations() -> List[Type[ManifestMigration]]:
    """
    Discovers and returns a sorted list of all ManifestMigration subclasses available in the migrations package.
    This function inspects the main migrations package and its submodules to find all classes that are subclasses of ManifestMigration,
    excluding the ManifestMigration base class itself and any duplicates. The discovered migration classes are then sorted using the
    _migration_order_key function to ensure they are returned in the correct order.

    Returns:
        List[Type[ManifestMigration]]: A list of discovered ManifestMigration subclasses, sorted by migration order.
    """

    migration_classes = []
    for _, obj in inspect.getmembers(sys.modules[migrations_pkg.__name__], inspect.isclass):
        if (
            issubclass(obj, ManifestMigration)
            and obj is not ManifestMigration
            and obj not in migration_classes
        ):
            migration_classes.append(obj)

    for _, module_name, _ in pkgutil.iter_modules(migrations_pkg.__path__):
        module = sys.modules.get(f"{migrations_pkg.__name__}.{module_name}")
        if module:
            for _, obj in inspect.getmembers(module, inspect.isclass):
                if (
                    issubclass(obj, ManifestMigration)
                    and obj is not ManifestMigration
                    and obj not in migration_classes
                ):
                    migration_classes.append(obj)

    # Sort by migration order key
    migration_classes.sort(key=_migration_order_key)

    return migration_classes


# registered migrations
MIGRATIONS: List[Type[ManifestMigration]] = _discover_migrations()
