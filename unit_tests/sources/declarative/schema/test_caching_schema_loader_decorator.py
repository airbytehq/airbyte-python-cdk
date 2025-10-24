from unittest import TestCase
from unittest.mock import Mock

from airbyte_cdk.sources.declarative.schema import SchemaLoader
from airbyte_cdk.sources.declarative.schema.caching_schema_loader_decorator import CachingSchemaLoaderDecorator


class CachingSchemaLoaderDecoratorTest(TestCase):
    def test_given_previous_calls_when_get_json_schema_then_return_cached_schema(self):
        decorated = Mock(spec=SchemaLoader)
        schema_loader = CachingSchemaLoaderDecorator(decorated)

        schema_loader.get_json_schema()
        schema_loader.get_json_schema()
        schema_loader.get_json_schema()

        assert decorated.get_json_schema.call_count == 1
