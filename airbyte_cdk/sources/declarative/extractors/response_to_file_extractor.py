#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging
import os
import uuid
import zlib
from contextlib import closing
from dataclasses import InitVar, dataclass
from enum import Enum
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

import pandas as pd
import requests
from numpy import nan

from airbyte_cdk.sources.declarative.extractors.record_extractor import RecordExtractor

EMPTY_STR: str = ""
DEFAULT_ENCODING: str = "utf-8"
DOWNLOAD_CHUNK_SIZE: int = 1024 * 10


class FileTypes(Enum):
    CSV = "csv"
    JSONL = "jsonl"


@dataclass
class ResponseToFileExtractor(RecordExtractor):
    """
    This class is used when having very big HTTP responses (usually streamed),
    which would require too much memory so we use disk space as a tradeoff.

    The extractor does the following:
        1) Save the response to a temporary file
        2) Read from the temporary file by chunks to avoid OOM
        3) Remove the temporary file after reading
        4) Return the records
        5) If the response is not compressed, it will be filtered for null bytes
        6) If the response is compressed, it will be decompressed
        7) If the response is compressed and contains null bytes, it will be filtered for null bytes

    """

    parameters: InitVar[Mapping[str, Any]]
    file_type: Optional[str] = "csv"

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        self.logger = logging.getLogger("airbyte")

    def extract_records(
        self, response: Optional[requests.Response] = None
    ) -> Iterable[Mapping[str, Any]]:
        """
        Extracts records from the given response by:
            1) Saving the result to a tmp file
            2) Reading from saved file by chunks to avoid OOM

        Args:
            response (Optional[requests.Response]): The response object containing the data. Defaults to None.

        Yields:
            Iterable[Mapping[str, Any]]: An iterable of mappings representing the extracted records.

        Returns:
            None
        """
        if response:
            file_path, encoding = self._save_to_file(response)
            yield from self._read_with_chunks(file_path, encoding)
        else:
            yield from []

    def _get_response_encoding(self, headers: Dict[str, Any]) -> str:
        """
        Get the encoding of the response based on the provided headers. This method is heavily inspired by the requests library
        implementation.

        Args:
            headers (Dict[str, Any]): The headers of the response.

        Returns:
            str: The encoding of the response.
        """

        content_type = headers.get("content-type")

        if not content_type:
            return DEFAULT_ENCODING

        content_type, params = requests.utils.parse_header_links(content_type)

        if "charset" in params:
            return params["charset"].strip("'\"")  # type: ignore  # we assume headers are returned as str

        return DEFAULT_ENCODING

    def _filter_null_bytes(self, b: bytes) -> bytes:
        """
        Filter out null bytes from a bytes object.

        Args:
            b (bytes): The input bytes object.
        Returns:
            bytes: The filtered bytes object with null bytes removed.

        Referenced Issue:
            https://github.com/airbytehq/airbyte/issues/8300
        """

        res = b.replace(b"\x00", b"")
        if len(res) < len(b):
            message = "ResponseToFileExtractor._filter_null_bytes(): Filter 'null' bytes from string, size reduced %d -> %d chars"
            self.logger.warning(message, len(b), len(res))
        return res

    def _get_file_path(self) -> str:
        """
        Get a temporary file path with a unique name.

        Returns:
            str: The path to the temporary file.

        Raises:
            ValueError: If the file type is not supported.
        """

        if self.file_type not in [file_type.value for file_type in FileTypes]:
            raise ValueError(
                f"ResponseToFileExtractor._get_file_path(): File type {self.file_type} is not supported.",
            )

        return str(uuid.uuid4()) + "." + self.file_type

    def _save_to_file(self, response: requests.Response) -> Tuple[str, str]:
        """
        Saves the binary data from the given response to a temporary file and returns the filepath and response encoding.

        Args:
            response (Optional[requests.Response]): The response object containing the binary data. Defaults to None.

        Returns:
            Tuple[str, str]: A tuple containing the filepath of the temporary file and the response encoding.

        Raises:
            ValueError: If the temporary file does not exist after saving the binary data.
        """
        # set filepath for binary data from response
        decompressor = zlib.decompressobj(zlib.MAX_WBITS | 32)
        needs_decompression = True  # we will assume at first that the response is compressed and change the flag if not

        file_path = self._get_file_path()
        # save binary data to tmp file
        with closing(response) as response, open(file_path, "wb") as data_file:
            response_encoding = self._get_response_encoding(dict(response.headers or {}))
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                try:
                    if needs_decompression:
                        data_file.write(decompressor.decompress(chunk))
                        needs_decompression = True
                    else:
                        data_file.write(self._filter_null_bytes(chunk))
                except zlib.error:
                    data_file.write(self._filter_null_bytes(chunk))
                    needs_decompression = False

        # check the file exists
        if os.path.isfile(file_path):
            return file_path, response_encoding
        else:
            message = "ResponseToFileExtractor._save_to_file(): The IO/Error occured while verifying binary data."
            raise ValueError(f"{message} Tmp file {file_path} doesn't exist.")

    def _read_csv(
        self,
        path: str,
        file_encoding: str,
        chunk_size: int = 100,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Reads a CSV file and yields each row as a dictionary.

        Args:
            path (str): The path to the CSV file to be read.
            file_encoding (str): The encoding of the file.

        Yields:
            Mapping[str, Any]: A dictionary representing each row of data.
        """

        csv_read_params = {
            "chunksize": chunk_size,
            "iterator": True,
            "dialect": "unix",
            "dtype": object,
            "encoding": file_encoding,
        }

        for chunk in pd.read_csv(path, **csv_read_params):
            # replace NaN with None
            chunk = chunk.replace({nan: None}).to_dict(orient="records")
            for record in chunk:
                yield record

    def _read_json_lines(
        self,
        path: str,
        file_encoding: str,
        chunk_size: int = 100,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Reads a JSON file and yields each row as a dictionary.

        Args:
            path (str): The path to the JSON file to be read.
            file_encoding (str): The encoding of the file.

        Yields:
            Mapping[str, Any]: A dictionary representing each row of data.
        """

        json_read_params = {
            "lines": True,
            "chunksize": chunk_size,
            "encoding": file_encoding,
            "convert_dates": False,
        }

        for chunk in pd.read_json(path, **json_read_params):
            for record in chunk.to_dict(orient="records"):
                yield record

    def _read_with_chunks(
        self,
        path: str,
        file_encoding: str,
        chunk_size: int = 100,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Reads data from a file in chunks and yields each row as a dictionary.

        Args:
            path (str): The path to the file to be read.
            file_encoding (str): The encoding of the file.
            chunk_size (int, optional): The size of each chunk to be read. Defaults to 100.

        Yields:
            Mapping[str, Any]: A dictionary representing each row of data.

        Raises:
            ValueError: If an error occurs while reading the data from the file.
        """

        try:
            if self.file_type == FileTypes.CSV.value:
                yield from self._read_csv(path, file_encoding, chunk_size)

            if self.file_type == FileTypes.JSONL.value:
                yield from self._read_json_lines(path, file_encoding, chunk_size)

        except pd.errors.EmptyDataError as e:
            message = "ResponseToFileExtractor._read_with_chunks(): Empty data received."
            self.logger.info(f"{message} {e}")
            yield from []
        except IOError as ioe:
            message = "ResponseToFileExtractor._read_with_chunks(): The IO/Error occured while reading the data from file."
            raise ValueError(f"{message} Called: {path}", ioe)
        finally:
            # remove binary tmp file, after data is read
            os.remove(path)
