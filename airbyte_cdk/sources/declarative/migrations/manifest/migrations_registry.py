import importlib
import inspect
import pkgutil
import re
import sys
from typing import List, Type

import airbyte_cdk.sources.declarative.migrations.manifest.migrations as migrations_pkg
from airbyte_cdk.sources.declarative.migrations.manifest.manifest_migration import (
    ManifestMigration,
)

# Dynamically import all modules in the migrations package
for _, module_name, is_pkg in pkgutil.iter_modules(migrations_pkg.__path__):
    if not is_pkg:
        importlib.import_module(f"{migrations_pkg.__name__}.{module_name}")


def _migration_order_key(cls: object) -> int:
    # Extract the migration order from the module name, e.g., 0_v6_45_2_http_requester_url_base_to_url_migration
    # The order is the integer at the start of the module name, before the first underscore
    module_name = cls.__module__.split(".")[-1]
    match = re.match(r"(\d+)_", module_name)
    return int(match.group(1)) if match else 0


def _discover_migrations() -> List[Type[ManifestMigration]]:
    migration_classes = []
    for name, obj in inspect.getmembers(sys.modules[migrations_pkg.__name__], inspect.isclass):
        if (
            issubclass(obj, ManifestMigration)
            and obj is not ManifestMigration
            and obj not in migration_classes
        ):
            migration_classes.append(obj)

    for _, module_name, _ in pkgutil.iter_modules(migrations_pkg.__path__):
        module = sys.modules.get(f"{migrations_pkg.__name__}.{module_name}")
        if module:
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if (
                    issubclass(obj, ManifestMigration)
                    and obj is not ManifestMigration
                    and obj not in migration_classes
                ):
                    migration_classes.append(obj)

    # Sort by migration order key
    migration_classes.sort(key=_migration_order_key)
    return migration_classes


MIGRATIONS: List[Type[ManifestMigration]] = _discover_migrations()
