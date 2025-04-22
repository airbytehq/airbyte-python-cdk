# Manifest Migrations

This directory contains the logic and registry for manifest migrations in the Airbyte CDK. Migrations are used to update or transform manifest components to newer formats or schemas as the CDK evolves.

## Adding a New Migration

1. **Create a Migration File:**
   - Add a new Python file in the `migrations/` subdirectory.
   - Name the file using the pattern: `<description>_v<major>_<minor>_<patch>__<order>.py`.
     - Example: `http_requester_url_base_to_url_v6_45_2__0.py`
   - The `<order>` integer is used to determine the order of migrations for the same version.

2. **Define the Migration Class:**
   - The migration class must inherit from `ManifestMigration`.
   - Name the class using the pattern: `V_<major>_<minor>_<patch>_<Description>`.
     - Example: `V_6_45_2_HttpRequesterUrlBaseToUrl`
   - Implement the following methods:
     - `should_migrate(self, manifest: ManifestType) -> bool`: Return `True` if the migration should be applied to the given manifest.
     - `migrate(self, manifest: ManifestType) -> None`: Perform the migration in-place.

3. **Migration Versioning:**
   - The migration version is extracted from the class name and used to determine applicability.
   - Only manifests with a version less than or equal to the migration version will be migrated.

4. **Component Type:**
   - Use the `TYPE_TAG` constant to check the component type in your migration logic.

5. **Examples:**
   - See `migrations/http_requester_url_base_to_url_v6_45_2__0.py` and `migrations/http_requester_path_to_url_v6_45_2__1.py` for reference implementations.

## Migration Registry

- All migration classes in the `migrations/` folder are automatically discovered and registered in `migrations_registry.py`.
- Migrations are applied in order, determined by the `<order>` suffix in the filename.

## Testing

- Ensure your migration is covered by unit tests.
- Tests should verify both `should_migrate` and `migrate` behaviors.

## Example Migration Skeleton

```python
from airbyte_cdk.sources.declarative.migrations.manifest.manifest_migration import TYPE_TAG, ManifestMigration, ManifestType

class V_1_2_3_Example(ManifestMigration):
    component_type = "ExampleComponent"
    original_key = "old_key"
    replacement_key = "new_key"

    def should_migrate(self, manifest: ManifestType) -> bool:
        return manifest[TYPE_TAG] == self.component_type and self.original_key in manifest

    def migrate(self, manifest: ManifestType) -> None:
        manifest[self.replacement_key] = manifest[self.original_key]
        manifest.pop(self.original_key, None)
```

## Additional Notes

- Do not modify the migration registry manually; it will pick up all valid migration classes automatically.
- If you need to skip certain component types, use the `NON_MIGRATABLE_TYPES` list in `manifest_migration.py`.

---

For more details, see the docstrings in `manifest_migration.py` and the examples in the `migrations/` folder.