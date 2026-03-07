# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
import csv
import os
from io import BytesIO
from pathlib import Path
from unittest import TestCase

import pytest
import requests
import requests_mock

from airbyte_cdk.sources.declarative.extractors import ResponseToFileExtractor


class ResponseToFileExtractorTest(TestCase):
    def setUp(self) -> None:
        self._extractor = ResponseToFileExtractor(parameters={})
        self._http_mocker = requests_mock.Mocker()
        self._http_mocker.__enter__()

    def tearDown(self) -> None:
        self._http_mocker.__exit__(None, None, None)

    def test_compressed_response(self) -> None:
        response = self._mock_streamed_response_from_file(self._compressed_response_path())
        extracted_records = list(self._extractor.extract_records(response))
        assert len(extracted_records) == 24

    def test_text_response(self) -> None:
        response = self._mock_streamed_response_from_file(self._decompressed_response_path())
        extracted_records = list(self._extractor.extract_records(response))
        assert len(extracted_records) == 24

    def test_text_response_with_null_bytes(self) -> None:
        csv_with_null_bytes = '"FIRST_\x00NAME","LAST_NAME"\n"a first n\x00ame","a last na\x00me"\n'
        response = self._mock_streamed_response(BytesIO(csv_with_null_bytes.encode("utf-8")))

        extracted_records = list(self._extractor.extract_records(response))

        assert extracted_records == [{"FIRST_NAME": "a first name", "LAST_NAME": "a last name"}]

    def test_tab_delimited_response(self) -> None:
        """Test that a tab-delimited (TSV) response is correctly parsed when delimiter is set to tab."""
        extractor = ResponseToFileExtractor(parameters={}, delimiter="\t")
        tsv_data = "Date\tApp Name\tApp Apple Identifier\tEvent\n2026-02-28\tBullseye Blast\t1632779218\tImpression\n"
        response = self._mock_streamed_response(BytesIO(tsv_data.encode("utf-8")))

        extracted_records = list(extractor.extract_records(response))

        assert len(extracted_records) == 1
        assert extracted_records[0] == {
            "Date": "2026-02-28",
            "App Name": "Bullseye Blast",
            "App Apple Identifier": "1632779218",
            "Event": "Impression",
        }

    def test_escaped_tab_delimiter(self) -> None:
        """Test that escaped tab delimiter (\\t from YAML) is correctly decoded."""
        extractor = ResponseToFileExtractor(parameters={}, delimiter="\\t")
        tsv_data = "col1\tcol2\tcol3\nval1\tval2\tval3\n"
        response = self._mock_streamed_response(BytesIO(tsv_data.encode("utf-8")))

        extracted_records = list(extractor.extract_records(response))

        assert len(extracted_records) == 1
        assert extracted_records[0] == {"col1": "val1", "col2": "val2", "col3": "val3"}

    def test_default_comma_delimiter(self) -> None:
        """Test that the default comma delimiter still works correctly."""
        extractor = ResponseToFileExtractor(parameters={})
        csv_data = "col1,col2,col3\nval1,val2,val3\n"
        response = self._mock_streamed_response(BytesIO(csv_data.encode("utf-8")))

        extracted_records = list(extractor.extract_records(response))

        assert len(extracted_records) == 1
        assert extracted_records[0] == {"col1": "val1", "col2": "val2", "col3": "val3"}

    def test_tab_delimiter_with_comma_in_values(self) -> None:
        """Test that commas in field values are preserved when using tab delimiter."""
        extractor = ResponseToFileExtractor(parameters={}, delimiter="\t")
        tsv_data = "name\taddress\nJohn Doe\t123 Main St, Apt 4\n"
        response = self._mock_streamed_response(BytesIO(tsv_data.encode("utf-8")))

        extracted_records = list(extractor.extract_records(response))

        assert len(extracted_records) == 1
        assert extracted_records[0] == {"name": "John Doe", "address": "123 Main St, Apt 4"}

    def _test_folder_path(self) -> Path:
        return Path(__file__).parent.resolve()

    def _compressed_response_path(self) -> Path:
        return self._test_folder_path() / "compressed_response"

    def _decompressed_response_path(self) -> Path:
        return self._test_folder_path() / "decompressed_response.csv"

    def _mock_streamed_response_from_file(self, path: Path) -> requests.Response:
        with path.open("rb") as f:
            return self._mock_streamed_response(f)  # type: ignore  # Could not find the right typing for file io

    def _mock_streamed_response(self, io: BytesIO) -> requests.Response:
        any_url = "https://anyurl.com"
        self._http_mocker.register_uri("GET", any_url, [{"body": io, "status_code": 200}])
        return requests.get(any_url)


@pytest.fixture(name="large_events_response")
def large_event_response_fixture():
    lines_in_response = 2_000_000  # ≈ 62 MB of response
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = f"{dir_path}/test_response.csv"
    with open(file_path, "w") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["username", "email"])  # headers
        for _ in range(lines_in_response):
            csv_writer.writerow(["a_username", "email1@example.com"])
    yield (lines_in_response, file_path)
    os.remove(file_path)


@pytest.mark.slow
@pytest.mark.limit_memory("20 MB")
def test_response_to_file_extractor_memory_usage(requests_mock, large_events_response):
    lines_in_response, file_path = large_events_response
    extractor = ResponseToFileExtractor(parameters={})

    url = "https://for-all-mankind.nasa.com/api/v1/users/users1"
    requests_mock.get(url, body=open(file_path, "rb"))

    counter = 0
    for _ in extractor.extract_records(requests.get(url, stream=True)):
        counter += 1

    assert counter == lines_in_response
