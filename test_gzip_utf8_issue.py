"""
Test script to reproduce the StreamThreadException issue with GZIP data and UTF-8 decoding.

This test demonstrates the root cause of issue #8301 where GZIP-compressed data
(starting with byte 0x8b) is incorrectly treated as UTF-8 text, causing the
'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte error.
"""

import gzip
import io
from unittest.mock import Mock

import requests

from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import (
    CompositeRawDecoder,
    CsvParser,
    GzipParser,
)


def test_gzip_utf8_decoding_issue():
    """
    Reproduce the issue where GZIP data is incorrectly treated as UTF-8.
    
    This simulates the scenario in Bing Ads campaign_labels stream where:
    1. Response contains GZIP-compressed CSV data
    2. Parser selection fails to detect GZIP content-encoding
    3. Compressed data is passed to UTF-8 decoder
    4. UTF-8 decoder fails with byte 0x8b error
    """
    csv_data = "Account Id,Campaign,Client Id\n123,Test Campaign,456\n"
    
    compressed_data = gzip.compress(csv_data.encode('utf-8'))
    
    assert compressed_data[1] == 0x8b, f"Expected GZIP magic number 0x8b, got {hex(compressed_data[1])}"
    
    mock_response = Mock(spec=requests.Response)
    mock_response.content = compressed_data
    mock_response.raw = io.BytesIO(compressed_data)
    mock_response.headers = {}  # Missing Content-Encoding: gzip header
    
    csv_parser = CsvParser(encoding="utf-8")
    decoder = CompositeRawDecoder(parser=csv_parser, stream_response=False)
    
    try:
        list(decoder.decode(mock_response))
        assert False, "Expected UTF-8 decoding error but none occurred"
    except UnicodeDecodeError as e:
        assert "can't decode byte 0x8b" in str(e)
        assert "invalid start byte" in str(e)
        print(f"✓ Reproduced the issue: {e}")
    
    gzip_parser = GzipParser(inner_parser=csv_parser)
    correct_decoder = CompositeRawDecoder(parser=gzip_parser, stream_response=False)
    
    mock_response.raw = io.BytesIO(compressed_data)
    
    records = list(correct_decoder.decode(mock_response))
    assert len(records) == 1
    assert records[0]["Account Id"] == "123"
    assert records[0]["Campaign"] == "Test Campaign"
    print("✓ Correct GZIP handling works as expected")


def test_header_based_parser_selection():
    """
    Test that CompositeRawDecoder.by_headers() correctly selects GZIP parser
    when Content-Encoding header is present.
    """
    csv_data = "Account Id,Campaign\n123,Test\n"
    compressed_data = gzip.compress(csv_data.encode('utf-8'))
    
    mock_response = Mock(spec=requests.Response)
    mock_response.content = compressed_data
    mock_response.raw = io.BytesIO(compressed_data)
    mock_response.headers = {"Content-Encoding": "gzip"}
    
    gzip_parser = GzipParser(inner_parser=CsvParser(encoding="utf-8"))
    fallback_parser = CsvParser(encoding="utf-8")
    
    decoder = CompositeRawDecoder.by_headers(
        parsers=[({"Content-Encoding"}, {"gzip"}, gzip_parser)],
        stream_response=False,
        fallback_parser=fallback_parser,
    )
    
    records = list(decoder.decode(mock_response))
    assert len(records) == 1
    assert records[0]["Account Id"] == "123"
    print("✓ Header-based parser selection works correctly")


if __name__ == "__main__":
    print("Testing GZIP UTF-8 decoding issue reproduction...")
    test_gzip_utf8_decoding_issue()
    test_header_based_parser_selection()
    print("All tests completed successfully!")
