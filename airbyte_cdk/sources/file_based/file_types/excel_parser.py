#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
import warnings
from io import IOBase
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

import orjson
import pandas as pd
from pydantic.v1 import BaseModel

from airbyte_cdk.sources.file_based.config.excel_format import SheetsToSync
from airbyte_cdk.sources.file_based.config.file_based_stream_config import (
    ExcelFormat,
    FileBasedStreamConfig,
)
from airbyte_cdk.sources.file_based.exceptions import (
    ConfigValidationError,
    ExcelCalamineParsingError,
    FileBasedSourceError,
    RecordParseError,
)
from airbyte_cdk.sources.file_based.file_based_stream_reader import (
    AbstractFileBasedStreamReader,
    FileReadMode,
)
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import SchemaType


class ExcelParser(FileTypeParser):
    ENCODING = None
    SHEET_NAME_COLUMN = "_ab_sheet_name"

    def check_config(self, config: FileBasedStreamConfig) -> Tuple[bool, Optional[str]]:
        """
        ExcelParser does not require config checks, implicit pydantic validation is enough.
        """
        return True, None

    async def infer_schema(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> SchemaType:
        """
        Infer the schema of the Excel file by examining its contents.
        """
        self.validate_format(config.format, logger)
        excel_format = self._as_excel_format(config.format, logger)

        fields: Dict[str, str] = {}

        with stream_reader.open_file(file, self.file_read_mode, self.ENCODING, logger) as fp:
            for df in self.parse_dataframes(fp, logger, file, excel_format):
                for column, df_type in df.dtypes.items():
                    prev_frame_column_type = fields.get(column)  # type: ignore [call-overload]
                    fields[column] = self.dtype_to_json_type(  # type: ignore [index]
                        prev_frame_column_type,
                        df_type,
                    )

        schema = {
            field: (
                {"type": "string", "format": "date-time"}
                if fields[field] == "date-time"
                else {"type": fields[field]}
            )
            for field in fields
        }
        return schema

    def parse_records(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        discovered_schema: Optional[Mapping[str, SchemaType]] = None,
    ) -> Iterable[Dict[str, Any]]:
        """
        Parse records from an Excel file with fallback error handling.
        """
        self.validate_format(config.format, logger)
        excel_format = self._as_excel_format(config.format, logger)

        try:
            with stream_reader.open_file(file, self.file_read_mode, self.ENCODING, logger) as fp:
                for df in self.parse_dataframes(fp, logger, file, excel_format):
                    # Yield records as dictionaries
                    # DataFrame.to_dict() method returns datetime values in pandas.Timestamp values, which are not serializable by orjson
                    # DataFrame.to_json() returns string with datetime values serialized to iso8601 with microseconds to align with pydantic behavior
                    # see PR description: https://github.com/airbytehq/airbyte/pull/44444/
                    yield from orjson.loads(
                        df.to_json(orient="records", date_format="iso", date_unit="us")
                    )

        except Exception as exc:
            # Raise a RecordParseError if any exception occurs during parsing
            raise RecordParseError(
                FileBasedSourceError.ERROR_PARSING_RECORD, filename=file.uri
            ) from exc

    @property
    def file_read_mode(self) -> FileReadMode:
        """
        Return the file read mode for Excel files.
        """
        return FileReadMode.READ_BINARY

    @staticmethod
    def dtype_to_json_type(
        current_type: Optional[str],
        dtype: Any,  # Type object from pandas DataFrame
    ) -> str:
        """
        Convert Pandas DataFrame types to Airbyte Types.

        Args:
            current_type (Optional[str]): One of the previous types based on earlier dataframes.
            dtype: Pandas DataFrame type.

        Returns:
            str: Corresponding Airbyte Type.
        """
        number_types = ("int64", "float64")
        if current_type == "string":
            # Previous column values were of the string type, no need to look further.
            return current_type
        if dtype is object:
            return "string"
        if dtype in number_types and (not current_type or current_type == "number"):
            return "number"
        if dtype == "bool" and (not current_type or current_type == "boolean"):
            return "boolean"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "date-time"
        return "string"

    @staticmethod
    def validate_format(excel_format: BaseModel, logger: logging.Logger) -> None:
        """
        Validate that the format is `ExcelFormat`.
        """
        if not isinstance(excel_format, ExcelFormat):
            logger.info(f"Expected ExcelFormat, got {excel_format}")
            raise ConfigValidationError(FileBasedSourceError.CONFIG_VALIDATION_ERROR)

    @staticmethod
    def _as_excel_format(excel_format: BaseModel, logger: logging.Logger) -> ExcelFormat:
        ExcelParser.validate_format(excel_format, logger)
        if isinstance(excel_format, ExcelFormat):
            return excel_format
        raise ConfigValidationError(FileBasedSourceError.CONFIG_VALIDATION_ERROR)

    def parse_dataframes(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
        excel_format: ExcelFormat,
    ) -> Iterable[pd.DataFrame]:
        if self._uses_legacy_first_sheet_only(excel_format):
            yield self.open_and_parse_file(fp, logger, file)
            return

        yield from self._parse_sheets(fp, logger, file, excel_format)

    @staticmethod
    def _uses_legacy_first_sheet_only(excel_format: ExcelFormat) -> bool:
        return (
            not excel_format.sheet_names
            and excel_format.sheets_to_sync == SheetsToSync.FIRST_SHEET_ONLY
        )

    def _parse_sheets(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
        excel_format: ExcelFormat,
    ) -> Iterable[pd.DataFrame]:
        try:
            yield from self._parse_sheets_with_calamine(fp, logger, file, excel_format)
        except ExcelCalamineParsingError:
            yield from self._parse_sheets_with_openpyxl(fp, logger, file, excel_format)

    def _parse_sheets_with_calamine(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
        excel_format: ExcelFormat,
    ) -> Iterable[pd.DataFrame]:
        try:
            with pd.ExcelFile(fp, engine="calamine") as workbook:  # type: ignore [arg-type]
                sheet_names = self._resolve_sheet_names(workbook, excel_format)
                yield from self._parse_sheets_from_workbook(workbook, sheet_names, file)
        except BaseException as exc:
            if "ValueError" in str(exc):
                logger.warning(
                    f"Calamine parsing failed for {file.file_uri_for_logging}, falling back to openpyxl: {exc}"
                )
                raise ExcelCalamineParsingError(
                    f"Calamine engine failed to parse {file.file_uri_for_logging}",
                    filename=file.uri,
                ) from exc
            raise exc

    def _parse_sheets_with_openpyxl(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
        excel_format: ExcelFormat,
    ) -> Iterable[pd.DataFrame]:
        # Some file-like objects are not seekable.
        if hasattr(fp, "seek"):
            try:
                fp.seek(0)  # type: ignore [union-attr]
            except OSError as exc:
                logger.info(
                    f"Could not rewind stream for {file.file_uri_for_logging}; "
                    f"proceeding with openpyxl from current position: {exc}"
                )

        with warnings.catch_warnings(record=True) as warning_records:
            warnings.simplefilter("always")
            with pd.ExcelFile(fp, engine="openpyxl") as workbook:  # type: ignore [arg-type]
                sheet_names = self._resolve_sheet_names(workbook, excel_format)
                dataframes = list(self._parse_sheets_from_workbook(workbook, sheet_names, file))

        for warning in warning_records:
            logger.warning(f"Openpyxl warning for {file.file_uri_for_logging}: {warning.message}")

        yield from dataframes

    def _parse_sheets_from_workbook(
        self,
        workbook: pd.ExcelFile,
        sheet_names: List[str],
        file: RemoteFile,
    ) -> Iterable[pd.DataFrame]:
        workbook_sheet_names = [str(sheet_name) for sheet_name in workbook.sheet_names]
        missing_sheet_names = sorted(set(sheet_names) - set(workbook_sheet_names))
        if missing_sheet_names:
            raise ValueError(
                f"Sheet names {missing_sheet_names} were not found in {file.file_uri_for_logging}"
            )

        for sheet_name in sheet_names:
            df = workbook.parse(sheet_name=sheet_name)  # type: ignore [call-overload]
            if self.SHEET_NAME_COLUMN in df.columns:
                raise ValueError(
                    f"Column {self.SHEET_NAME_COLUMN} is reserved for sheet metadata in {file.file_uri_for_logging}"
                )
            df[self.SHEET_NAME_COLUMN] = sheet_name
            yield df

    @staticmethod
    def _resolve_sheet_names(workbook: pd.ExcelFile, excel_format: ExcelFormat) -> List[str]:
        if excel_format.sheet_names:
            return excel_format.sheet_names
        if excel_format.sheets_to_sync == SheetsToSync.ALL_SHEETS:
            return [str(sheet_name) for sheet_name in workbook.sheet_names]
        return [str(sheet_name) for sheet_name in workbook.sheet_names[:1]]

    def _open_and_parse_file_with_calamine(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
    ) -> pd.DataFrame:
        """Opens and parses Excel file using Calamine engine.

        Args:
            fp: File pointer to the Excel file.
            logger: Logger for logging information and errors.
            file: Remote file information for logging context.

        Returns:
            pd.DataFrame: Parsed data from the Excel file.

        Raises:
            ExcelCalamineParsingError: If Calamine fails to parse the file.
        """
        try:
            return pd.ExcelFile(fp, engine="calamine").parse()  # type: ignore [arg-type, call-overload, no-any-return]
        except BaseException as exc:
            # Calamine engine raises PanicException(child of BaseException) if Calamine fails to parse the file.
            # Checking if ValueError in exception arg to know if it was actually an error during parsing due to invalid values in cells.
            # Otherwise, raise an exception.
            if "ValueError" in str(exc):
                logger.warning(
                    f"Calamine parsing failed for {file.file_uri_for_logging}, falling back to openpyxl: {exc}"
                )
                raise ExcelCalamineParsingError(
                    f"Calamine engine failed to parse {file.file_uri_for_logging}",
                    filename=file.uri,
                ) from exc
            raise exc

    def _open_and_parse_file_with_openpyxl(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
    ) -> pd.DataFrame:
        """Opens and parses Excel file using Openpyxl engine.

        Args:
            fp: File pointer to the Excel file.
            logger: Logger for logging information and errors.
            file: Remote file information for logging context.

        Returns:
            pd.DataFrame: Parsed data from the Excel file.
        """
        # Some file-like objects are not seekable.
        if hasattr(fp, "seek"):
            try:
                fp.seek(0)  # type: ignore [union-attr]
            except OSError as exc:
                logger.info(
                    f"Could not rewind stream for {file.file_uri_for_logging}; "
                    f"proceeding with openpyxl from current position: {exc}"
                )

        with warnings.catch_warnings(record=True) as warning_records:
            warnings.simplefilter("always")
            df = pd.ExcelFile(fp, engine="openpyxl").parse()  # type: ignore [arg-type, call-overload]

        for warning in warning_records:
            logger.warning(f"Openpyxl warning for {file.file_uri_for_logging}: {warning.message}")

        return df  # type: ignore [no-any-return]

    def open_and_parse_file(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
    ) -> pd.DataFrame:
        """Opens and parses the Excel file with Calamine-first and Openpyxl fallback.

        Args:
            fp: File pointer to the Excel file.
            logger: Logger for logging information and errors.
            file: Remote file information for logging context.

        Returns:
            pd.DataFrame: Parsed data from the Excel file.
        """
        try:
            return self._open_and_parse_file_with_calamine(fp, logger, file)
        except ExcelCalamineParsingError:
            return self._open_and_parse_file_with_openpyxl(fp, logger, file)
