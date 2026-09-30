#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
import warnings
from io import IOBase
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, Mapping, Optional, Tuple, Union

import orjson
import pandas as pd
from pydantic.v1 import BaseModel

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
    ALL_SHEETS = "*"
    ab_sheet_name_col = "_ab_source_sheet_name"

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
        Infers the schema of the Excel file by examining its contents.

        Args:
            config (FileBasedStreamConfig): Configuration for the file-based stream.
            file (RemoteFile): The remote file to be read.
            stream_reader (AbstractFileBasedStreamReader): Reader to read the file.
            logger (logging.Logger): Logger for logging information and errors.

        Returns:
            SchemaType: Inferred schema of the Excel file.
        """

        # Validate the format of the config
        self.validate_format(config.format, logger)
        excel_format = config.format
        if not isinstance(excel_format, ExcelFormat):
            raise ConfigValidationError(FileBasedSourceError.CONFIG_VALIDATION_ERROR)

        fields: Dict[str, str] = {}

        with stream_reader.open_file(file, self.file_read_mode, self.ENCODING, logger) as fp:
            for _, df in self._iter_worksheets(fp, excel_format, logger, file):
                for column, df_type in df.dtypes.items():
                    prev_frame_column_type = fields.get(column)  # type: ignore [call-overload]
                    fields[column] = self.dtype_to_json_type(  # type: ignore [index]
                        prev_frame_column_type,
                        df_type,
                    )

        if self._reads_all_sheets(excel_format):
            fields[self.ab_sheet_name_col] = "string"

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
        Parses records from an Excel file with fallback error handling.

        Args:
            config (FileBasedStreamConfig): Configuration for the file-based stream.
            file (RemoteFile): The remote file to be read.
            stream_reader (AbstractFileBasedStreamReader): Reader to read the file.
            logger (logging.Logger): Logger for logging information and errors.
            discovered_schema (Optional[Mapping[str, SchemaType]]): Discovered schema for validation.

        Yields:
            Iterable[Dict[str, Any]]: Parsed records from the Excel file.
        """

        # Validate the format of the config
        self.validate_format(config.format, logger)
        excel_format = config.format
        if not isinstance(excel_format, ExcelFormat):
            raise ConfigValidationError(FileBasedSourceError.CONFIG_VALIDATION_ERROR)

        include_sheet_name = self._reads_all_sheets(excel_format)

        try:
            # Open and parse the file using the stream reader
            with stream_reader.open_file(file, self.file_read_mode, self.ENCODING, logger) as fp:
                for sheet, df in self._iter_worksheets(fp, excel_format, logger, file):
                    # DataFrame.to_dict() returns pandas.Timestamp values not serializable by orjson.
                    # DataFrame.to_json() serializes datetimes to iso8601 with microseconds.
                    records = orjson.loads(
                        df.to_json(orient="records", date_format="iso", date_unit="us")
                    )
                    if include_sheet_name:
                        for record in records:
                            record[self.ab_sheet_name_col] = sheet
                    yield from records

        except ConfigValidationError:
            # A missing worksheet is a configuration problem, not an unparseable record.
            raise
        except Exception as exc:
            # Raise a RecordParseError if any exception occurs during parsing
            raise RecordParseError(
                FileBasedSourceError.ERROR_PARSING_RECORD, filename=file.uri
            ) from exc

    @property
    def file_read_mode(self) -> FileReadMode:
        """
        Returns the file read mode for the Excel file.

        Returns:
            FileReadMode: The file read mode (binary).
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
        Validates if the given format is of type ExcelFormat.

        Args:
            excel_format (Any): The format to be validated.

        Raises:
            ConfigValidationError: If the format is not ExcelFormat.
        """
        if not isinstance(excel_format, ExcelFormat):
            logger.info(f"Expected ExcelFormat, got {excel_format}")
            raise ConfigValidationError(FileBasedSourceError.CONFIG_VALIDATION_ERROR)

    def _open_and_parse_file_with_calamine(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
        sheet_name: Union[int, str, None] = 0,
    ) -> Union[pd.DataFrame, Dict[Union[int, str], pd.DataFrame]]:
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
            return pd.ExcelFile(fp, engine="calamine").parse(sheet_name=sheet_name)  # type: ignore [arg-type, call-overload, no-any-return]
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
        sheet_name: Union[int, str, None] = 0,
    ) -> Union[pd.DataFrame, Dict[Union[int, str], pd.DataFrame]]:
        """Opens and parses Excel file using Openpyxl engine.

        Args:
            fp: File pointer to the Excel file.
            logger: Logger for logging information and errors.
            file: Remote file information for logging context.

        Returns:
            pd.DataFrame: Parsed data from the Excel file.
        """
        self._rewind(fp, logger, file)
        return self._with_openpyxl_warnings_logged(  # type: ignore [no-any-return]
            lambda: pd.ExcelFile(fp, engine="openpyxl").parse(sheet_name=sheet_name),  # type: ignore [arg-type, call-overload]
            logger,
            file,
        )

    @staticmethod
    def _rewind(fp: Union[IOBase, str, Path], logger: logging.Logger, file: RemoteFile) -> None:
        """Rewinds the stream before handing it to a second engine, where possible."""
        # Some file-like objects are not seekable.
        if hasattr(fp, "seek"):
            try:
                fp.seek(0)  # type: ignore [union-attr]
            except OSError as exc:
                logger.info(
                    f"Could not rewind stream for {file.file_uri_for_logging}; "
                    f"proceeding with openpyxl from current position: {exc}"
                )

    @staticmethod
    def _with_openpyxl_warnings_logged(
        call: Callable[[], Any], logger: logging.Logger, file: RemoteFile
    ) -> Any:
        """Runs an openpyxl call, surfacing the warnings it raises as log lines."""
        with warnings.catch_warnings(record=True) as warning_records:
            warnings.simplefilter("always")
            result = call()

        for warning in warning_records:
            logger.warning(f"Openpyxl warning for {file.file_uri_for_logging}: {warning.message}")

        return result

    def open_and_parse_file(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
        sheet_name: Union[int, str, None] = 0,
    ) -> Union[pd.DataFrame, Dict[Union[int, str], pd.DataFrame]]:
        """Opens and parses the Excel file with Calamine-first and Openpyxl fallback.

        Args:
            fp: File pointer to the Excel file.
            logger: Logger for logging information and errors.
            file: Remote file information for logging context.

        Returns:
            pd.DataFrame: Parsed data from the Excel file.
        """
        try:
            return self._open_and_parse_file_with_calamine(fp, logger, file, sheet_name)
        except ExcelCalamineParsingError:
            return self._open_and_parse_file_with_openpyxl(fp, logger, file, sheet_name)

    def _iter_worksheets(
        self,
        fp: Union[IOBase, str, Path],
        excel_format: ExcelFormat,
        logger: logging.Logger,
        file: RemoteFile,
    ) -> Iterator[Tuple[Union[int, str], pd.DataFrame]]:
        """Yields (worksheet name, frame), one worksheet at a time.

        Callers always see the same shape whether one worksheet or all of them were
        selected, and the frames arrive lazily so a caller that streams records can let
        each one go before the next is parsed.
        """
        sheet_name = self._resolve_sheet_name(excel_format)
        if sheet_name is not None:
            yield self._parse_one_worksheet(fp, excel_format, sheet_name, logger, file)
            return
        yield from self._iter_every_worksheet(fp, logger, file)

    def _parse_one_worksheet(
        self,
        fp: Union[IOBase, str, Path],
        excel_format: ExcelFormat,
        sheet_name: Union[int, str],
        logger: logging.Logger,
        file: RemoteFile,
    ) -> Tuple[Union[int, str], pd.DataFrame]:
        """Parses the single worksheet the config selected."""
        try:
            parsed = self.open_and_parse_file(fp, logger, file, sheet_name)
        except ValueError as exc:
            # pandas raises ValueError("Worksheet named 'x' not found") for both engines.
            if "not found" not in str(exc):
                raise
            raise ConfigValidationError(
                f"Worksheet {excel_format.sheet_name!r} was not found in the workbook. "
                f"{self._describe_available_sheets(fp, logger)}"
                "Worksheet names are case-sensitive. "
                f'Set the "Sheet Name" option to an exact worksheet name, or to "*" to read '
                "every worksheet.",
                filename=file.uri,
            ) from exc
        return (sheet_name if isinstance(sheet_name, str) else 0), parsed  # type: ignore [return-value]

    def _iter_every_worksheet(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
    ) -> Iterator[Tuple[Union[int, str], pd.DataFrame]]:
        """Parses each worksheet in turn so only one frame is held at a time.

        Asking pandas for every worksheet at once (`sheet_name=None`) materializes one
        frame per worksheet up front, so peak memory tracks the whole workbook rather
        than its largest sheet.

        The engine fallback only covers the first worksheet. Once an earlier worksheet's
        records are downstream, re-reading the workbook with openpyxl would emit them a
        second time, so a later failure is left to propagate. The stream logs it and
        retries the file on the next sync, which is how every other streaming parser
        already behaves on a mid-file error.
        """
        try:
            yield from self._iter_worksheets_with_calamine(fp, logger, file)
            return
        except ExcelCalamineParsingError:
            pass
        yield from self._iter_worksheets_with_openpyxl(fp, logger, file)

    def _iter_worksheets_with_calamine(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
    ) -> Iterator[Tuple[Union[int, str], pd.DataFrame]]:
        """Walks the workbook with Calamine, holding the handle open across worksheets."""
        workbook = pd.ExcelFile(fp, engine="calamine")  # type: ignore [arg-type]
        emitted = False
        for name in workbook.sheet_names:
            try:
                df = workbook.parse(sheet_name=name)
            except BaseException as exc:
                # Calamine raises PanicException (a BaseException) on a cell it cannot
                # read. Falling back is only safe while nothing has been emitted yet.
                if not emitted and "ValueError" in str(exc):
                    logger.warning(
                        f"Calamine parsing failed for {file.file_uri_for_logging}, falling back to openpyxl: {exc}"
                    )
                    raise ExcelCalamineParsingError(
                        f"Calamine engine failed to parse {file.file_uri_for_logging}",
                        filename=file.uri,
                    ) from exc
                raise
            emitted = True
            yield name, df

    def _iter_worksheets_with_openpyxl(
        self,
        fp: Union[IOBase, str, Path],
        logger: logging.Logger,
        file: RemoteFile,
    ) -> Iterator[Tuple[Union[int, str], pd.DataFrame]]:
        """Walks the workbook with Openpyxl, holding the handle open across worksheets."""
        self._rewind(fp, logger, file)
        workbook = self._with_openpyxl_warnings_logged(
            lambda: pd.ExcelFile(fp, engine="openpyxl"),  # type: ignore [arg-type]
            logger,
            file,
        )
        for name in workbook.sheet_names:
            yield (
                name,
                self._with_openpyxl_warnings_logged(
                    lambda sheet=name: workbook.parse(sheet_name=sheet),  # type: ignore [misc]
                    logger,
                    file,
                ),
            )

    def _resolve_sheet_name(self, excel_format: ExcelFormat) -> Union[int, str, None]:
        """Converts the config value to a pandas-compatible `sheet_name` argument.

        Unset means the first worksheet, which is the behavior that predates this option.
        `*` means every worksheet; pandas spells that `None`. Any other value is used as a
        literal worksheet name -- notably, a numeric-looking name like "2026" stays a name
        rather than becoming a positional index.
        """
        value = excel_format.sheet_name
        if value is None:
            return 0
        if value == self.ALL_SHEETS:
            return None
        return value

    def _reads_all_sheets(self, excel_format: ExcelFormat) -> bool:
        """Whether the config selects every worksheet in the workbook."""
        return excel_format.sheet_name == self.ALL_SHEETS

    @staticmethod
    def _describe_available_sheets(fp: Union[IOBase, str, Path], logger: logging.Logger) -> str:
        """Best-effort listing of the worksheets present, for the not-found error message."""
        try:
            if hasattr(fp, "seek"):
                fp.seek(0)  # type: ignore [union-attr]
            names = pd.ExcelFile(fp, engine="calamine").sheet_names  # type: ignore [arg-type]
        except Exception as exc:
            logger.info(f"Could not list worksheets for the error message: {exc}")
            return ""
        return f"Available worksheets: {', '.join(repr(str(n)) for n in names)}. "
