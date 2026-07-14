# Spike Investigation: StreamThreadException in Bing Ads Source

## Issue Summary
- **Issue**: [#8301](https://github.com/airbytehq/oncall/issues/8301) - StreamThreadException in Bing Ads source
- **Error**: `'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte`
- **Stream**: `campaign_labels`
- **Root Cause**: GZIP-compressed data being treated as UTF-8 text

## Analysis

### Error Context
From Christo's clarification in the issue:
```
Exception while syncing stream campaign_labels: 'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte
```

The byte `0x8b` is the GZIP magic number, indicating that compressed data is being passed to a UTF-8 decoder.

### Technical Investigation

#### 1. Bing Ads Connector Configuration
- Uses `GzipDecoder` with `CsvDecoder` for bulk streams
- Encoding: `utf-8-sig`
- Stream: `campaign_labels` with `DownloadEntities: ["CampaignLabels"]`

#### 2. Concurrent Source Framework
- `StreamThreadException` wraps exceptions from concurrent processing
- `CompositeRawDecoder` handles response decoding with multiple parsers
- `GzipParser` decompresses GZIP data before passing to inner parsers

#### 3. Root Cause Analysis
The issue occurs in the concurrent source framework when:
1. GZIP-compressed response is received
2. Parser selection logic fails to detect GZIP content-encoding
3. Compressed data (starting with 0x8b) is passed directly to UTF-8 decoder
4. UTF-8 decoder fails with the observed error
5. Exception is wrapped in `StreamThreadException`

## Proposed Investigation Areas

### 1. Parser Selection Logic
- Examine `CompositeRawDecoder._select_parser()` method
- Check header-based parser selection for GZIP content
- Investigate concurrent source integration with declarative decoders

### 2. Error Handling
- Review exception propagation in concurrent processing
- Check if GZIP decompression errors are properly handled
- Examine fallback mechanisms for parser failures

### 3. Integration Points
- Analyze how `ConcurrentDeclarativeSource` handles bulk streams
- Check if declarative decoders are properly integrated with concurrent framework
- Investigate state management during concurrent processing

## Next Steps

1. Create test cases to reproduce the issue
2. Implement parser selection improvements
3. Add better error handling for GZIP decompression
4. Test with Bing Ads campaign_labels stream
5. Validate fix doesn't break other connectors

## Files to Investigate
- `airbyte_cdk/sources/declarative/decoders/composite_raw_decoder.py`
- `airbyte_cdk/sources/concurrent_source/concurrent_read_processor.py`
- `airbyte_cdk/sources/declarative/concurrent_declarative_source.py`
- Bing Ads manifest configuration for bulk streams
