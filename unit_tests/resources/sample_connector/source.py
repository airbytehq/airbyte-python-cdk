from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream


class SampleStream(Stream):
    name = "sample"
    
    def read_records(self, *args, **kwargs):
        yield {"id": 1, "name": "Test"}


class SourceSampleConnector(AbstractSource):
    def check_connection(self, logger, config):
        return True, None
    
    def streams(self, config):
        return [SampleStream()]
