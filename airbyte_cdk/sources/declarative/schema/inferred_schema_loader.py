#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

from collections.abc import Mapping as ABCMapping
from collections.abc import Sequence
from dataclasses import InitVar, dataclass
from typing import Any, Mapping, Optional

from airbyte_cdk.models import AirbyteRecordMessage
from airbyte_cdk.sources.declarative.retrievers.retriever import Retriever
from airbyte_cdk.sources.declarative.schema.schema_loader import SchemaLoader
from airbyte_cdk.sources.types import Config
from airbyte_cdk.utils.schema_inferrer import SchemaInferrer


def _to_builtin_types(value: Any) -> Any:
    """
    Recursively convert Mapping-like and Sequence-like objects to plain Python types.

    This is necessary because genson's schema inference doesn't handle custom Mapping
    or Sequence implementations properly. We need to convert everything to plain dicts,
    lists, and primitive types.

    Args:
        value: The value to convert

    Returns:
        The value converted to plain Python types
    """
    if isinstance(value, ABCMapping):
        return {k: _to_builtin_types(v) for k, v in value.items()}
    elif isinstance(value, (list, tuple)):
        return [_to_builtin_types(item) for item in value]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_to_builtin_types(item) for item in value]
    else:
        return value


@dataclass
class InferredSchemaLoader(SchemaLoader):
    """
    Infers a JSON Schema by reading a sample of records from the stream at discover time.

    This schema loader reads up to `record_sample_size` records from the stream and uses
    the SchemaInferrer to generate a JSON schema based on the structure of those records.
    This is useful for streams where the schema is not known in advance or changes dynamically.

    Attributes:
        retriever (Retriever): The retriever used to fetch records from the stream
        config (Config): The user-provided configuration as specified by the source's spec
        parameters (Mapping[str, Any]): Additional arguments to pass to the string interpolation if needed
        record_sample_size (int): The maximum number of records to read for schema inference. Defaults to 100.
        stream_name (str): The name of the stream for which to infer the schema
    """

    retriever: Retriever
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    record_sample_size: int = 100
    stream_name: str = ""

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self._parameters = parameters
        if not self.stream_name:
            self.stream_name = parameters.get("name", "")

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Infers and returns a JSON schema by reading a sample of records from the stream.

        This method reads up to `record_sample_size` records from the stream and uses
        the SchemaInferrer to generate a JSON schema. If no records are available,
        it returns an empty schema.

        Returns:
            A mapping representing the inferred JSON schema for the stream
        """
        schema_inferrer = SchemaInferrer()

        record_count = 0
        for stream_slice in self.retriever.stream_slices():
            for record in self.retriever.read_records(records_schema={}, stream_slice=stream_slice):
                if record_count >= self.record_sample_size:
                    break

                # Convert all Mapping-like and Sequence-like objects to plain Python types
                # This is necessary because genson doesn't handle custom implementations properly
                record = _to_builtin_types(record)

                airbyte_record = AirbyteRecordMessage(
                    stream=self.stream_name,
                    data=record,  # type: ignore[arg-type]
                    emitted_at=0,
                )

                schema_inferrer.accumulate(airbyte_record)
                record_count += 1

            if record_count >= self.record_sample_size:
                break

        inferred_schema: Optional[Mapping[str, Any]] = schema_inferrer.get_stream_schema(
            self.stream_name
        )

        return inferred_schema if inferred_schema else {}
