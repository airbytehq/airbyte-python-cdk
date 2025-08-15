"""
Proposed fix for StreamThreadException in Bing Ads source connector.

This fix addresses the root cause where GZIP-compressed data is incorrectly
treated as UTF-8 text due to missing Content-Encoding header detection
in the concurrent source framework.

Issue: #8301 - 'utf-8' codec can't decode byte 0x8b in position 1: invalid start byte
"""

import io
import logging
from typing import Any, Dict, Optional

import requests

from airbyte_cdk.sources.declarative.decoders.composite_raw_decoder import (
    CompositeRawDecoder,
    CsvParser,
    GzipParser,
    Parser,
)

logger = logging.getLogger("airbyte")


class ImprovedCompositeRawDecoder(CompositeRawDecoder):
    """
    Enhanced CompositeRawDecoder with better GZIP detection and error handling.
    
    This addresses the StreamThreadException issue by:
    1. Auto-detecting GZIP content based on magic bytes
    2. Providing better error handling for decompression failures
    3. Falling back gracefully when parser selection fails
    """
    
    def __init__(
        self,
        parser: Parser,
        stream_response: bool = True,
        parsers_by_header: Optional[Dict[str, Any]] = None,
        auto_detect_gzip: bool = True,
    ) -> None:
        super().__init__(parser, stream_response, parsers_by_header)
        self._auto_detect_gzip = auto_detect_gzip
    
    def _detect_gzip_content(self, response: requests.Response) -> bool:
        """
        Detect if response content is GZIP-compressed by checking magic bytes.
        
        Returns True if the response starts with GZIP magic number (0x1f, 0x8b).
        This helps identify GZIP content even when Content-Encoding header is missing.
        """
        if not self._auto_detect_gzip:
            return False
            
        try:
            if hasattr(response, 'raw') and response.raw:
                current_pos = response.raw.tell() if hasattr(response.raw, 'tell') else None
                
                magic_bytes = response.raw.read(2)
                
                if current_pos is not None and hasattr(response.raw, 'seek'):
                    response.raw.seek(current_pos)
                elif hasattr(response.raw, 'seek'):
                    response.raw.seek(0)
                
                return len(magic_bytes) >= 2 and magic_bytes[0] == 0x1f and magic_bytes[1] == 0x8b
            
            elif hasattr(response, 'content') and len(response.content) >= 2:
                return response.content[0] == 0x1f and response.content[1] == 0x8b
                
        except Exception as e:
            logger.debug(f"Failed to detect GZIP content: {e}")
            
        return False
    
    def _select_parser(self, response: requests.Response) -> Parser:
        """
        Enhanced parser selection with GZIP auto-detection.
        
        This method extends the base implementation to:
        1. Check Content-Encoding header (existing behavior)
        2. Auto-detect GZIP content by magic bytes
        3. Wrap parser with GzipParser if GZIP is detected
        """
        selected_parser = super()._select_parser(response)
        
        if (not isinstance(selected_parser, GzipParser) and 
            self._detect_gzip_content(response)):
            
            logger.info("Auto-detected GZIP content without Content-Encoding header, wrapping parser")
            
            return GzipParser(inner_parser=selected_parser)
        
        return selected_parser
    
    def decode(self, response: requests.Response):
        """
        Enhanced decode method with better error handling.
        
        Provides more informative error messages and graceful fallback
        when decompression or parsing fails.
        """
        try:
            yield from super().decode(response)
        except UnicodeDecodeError as e:
            if "can't decode byte 0x8b" in str(e):
                error_msg = (
                    f"UTF-8 decoding failed with GZIP magic byte 0x8b. "
                    f"This suggests GZIP-compressed data is being treated as UTF-8 text. "
                    f"Check Content-Encoding headers or enable auto_detect_gzip. "
                    f"Original error: {e}"
                )
                logger.error(error_msg)
                
                if self._auto_detect_gzip and self._detect_gzip_content(response):
                    logger.info("Attempting recovery with GZIP decompression")
                    gzip_parser = GzipParser(inner_parser=self.parser)
                    
                    if hasattr(response, 'raw') and hasattr(response.raw, 'seek'):
                        response.raw.seek(0)
                    
                    try:
                        if self.is_stream_response():
                            response.raw.auto_close = False
                            yield from gzip_parser.parse(data=response.raw)
                            response.raw.close()
                        else:
                            yield from gzip_parser.parse(data=io.BytesIO(response.content))
                        return
                    except Exception as recovery_error:
                        logger.error(f"GZIP recovery failed: {recovery_error}")
                
                raise RuntimeError(error_msg) from e
            else:
                raise
        except Exception as e:
            logger.error(f"Decoder error: {e}")
            raise


def create_bing_ads_compatible_decoder() -> ImprovedCompositeRawDecoder:
    """
    Create a CompositeRawDecoder configured for Bing Ads bulk streams.
    
    This decoder handles the campaign_labels stream and other bulk streams
    that use GZIP compression with CSV data.
    """
    csv_parser = CsvParser(encoding="utf-8-sig", set_values_to_none=[""])
    
    gzip_parser = GzipParser(inner_parser=csv_parser)
    
    decoder = ImprovedCompositeRawDecoder.by_headers(
        parsers=[({"Content-Encoding"}, {"gzip"}, gzip_parser)],
        stream_response=True,
        fallback_parser=csv_parser,
    )
    
    decoder._auto_detect_gzip = True
    
    return decoder


if __name__ == "__main__":
    print("Proposed fix for StreamThreadException in Bing Ads source")
    print("This enhanced CompositeRawDecoder provides:")
    print("1. Auto-detection of GZIP content by magic bytes")
    print("2. Better error handling for UTF-8/GZIP issues")
    print("3. Graceful fallback and recovery mechanisms")
    print("4. Specific configuration for Bing Ads bulk streams")
