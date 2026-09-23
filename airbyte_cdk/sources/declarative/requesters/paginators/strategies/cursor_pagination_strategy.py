#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import InitVar, dataclass, field
from typing import Any, Dict, Mapping, Optional, Union

import requests

from airbyte_cdk.sources.declarative.decoders import (
    Decoder,
    JsonDecoder,
    PaginationDecoderDecorator,
)
from airbyte_cdk.sources.declarative.interpolation.interpolated_boolean import InterpolatedBoolean
from airbyte_cdk.sources.declarative.interpolation.interpolated_string import InterpolatedString
from airbyte_cdk.sources.declarative.parsers.stop_condition_safety import (
    REQUESTED_PAGE_SIZE_VARIABLE,
    references_requested_page_size,
)
from airbyte_cdk.sources.declarative.requesters.paginators.strategies.pagination_strategy import (
    PaginationStrategy,
)
from airbyte_cdk.sources.types import Config, Record


@dataclass
class CursorPaginationStrategy(PaginationStrategy):
    """
    Pagination strategy that evaluates an interpolated string to define the next page token.

    Attributes:
        page_size (Optional[Union[str, int]]): the number of records to request
        cursor_value (Union[InterpolatedString, str]): template string evaluating to the cursor value
        config (Config): connection config
        stop_condition (Optional[InterpolatedBoolean]): template string evaluating when to stop paginating
        decoder (Decoder): decoder to decode the response
    """

    cursor_value: Union[InterpolatedString, str]
    config: Config
    parameters: InitVar[Mapping[str, Any]]
    page_size: Optional[Union[str, int]] = None
    stop_condition: Optional[Union[InterpolatedBoolean, str]] = None
    decoder: Decoder = field(
        default_factory=lambda: PaginationDecoderDecorator(decoder=JsonDecoder(parameters={}))
    )

    def __post_init__(self, parameters: Mapping[str, Any]) -> None:
        if isinstance(self.cursor_value, str):
            self._cursor_value = InterpolatedString.create(self.cursor_value, parameters=parameters)
        else:
            self._cursor_value = self.cursor_value
        if isinstance(self.stop_condition, str):
            self._stop_condition: Optional[InterpolatedBoolean] = InterpolatedBoolean(
                condition=self.stop_condition, parameters=parameters
            )
        else:
            self._stop_condition = self.stop_condition

        if isinstance(self.page_size, int) or (self.page_size is None):
            self._page_size = self.page_size
        else:
            page_size = InterpolatedString(self.page_size, parameters=parameters).eval(self.config)
            if not isinstance(page_size, int):
                raise Exception(f"{page_size} is of type {type(page_size)}. Expected {int}")
            self._page_size = page_size

        if self._page_size is None:
            self._reject_unbound_page_size_variable()

    def _reject_unbound_page_size_variable(self) -> None:
        """
        Fail construction when an expression reads `page_size` while the strategy declares none.

        The variable holds the page size that was actually requested, so it is the one safe thing to compare
        `last_page_size` against while a `page_size_reduction` is shrinking the page. Without a declared
        `page_size` there is nothing to bind it to, and an unbound comparison does not raise: Jinja fails, the
        interpolation falls back to the raw template string, and a non-empty string is truthy. A
        `stop_condition` written that way stops after the first page and drops the rest of the partition
        without failing, which is why this is a construction error rather than a warning.
        """
        for field_name, template in (
            ("stop_condition", self.stop_condition),
            ("cursor_value", self.cursor_value),
        ):
            expression = template.string if isinstance(template, InterpolatedString) else template
            if isinstance(template, InterpolatedBoolean):
                expression = template.condition
            if not isinstance(expression, str) or not references_requested_page_size(expression):
                continue
            raise ValueError(
                f"The `{field_name}` {expression!r} reads the `{REQUESTED_PAGE_SIZE_VARIABLE}` interpolation "
                f"variable, but the CursorPagination strategy it belongs to declares no `page_size`, so there "
                f"is nothing to bind it to. The comparison would not fail either - it renders as the template "
                f"string itself, which is truthy - so the pagination would end after the first page and the "
                f"rest of the partition would be dropped silently. Declare `page_size` on the pagination "
                f"strategy, or compare against a value the manifest defines, such as "
                f"`config['page_size']`."
            )

    @property
    def initial_token(self) -> Optional[Any]:
        """
        CursorPaginationStrategy does not have an initial value because the next cursor is typically included
        in the response of the first request. For Resumable Full Refresh streams that checkpoint the page
        cursor, the next cursor should be read from the state or stream slice object.
        """
        return None

    def next_page_token(
        self,
        response: requests.Response,
        last_page_size: int,
        last_record: Optional[Record],
        last_page_token_value: Optional[Any] = None,
        page_size_override: Optional[int] = None,
    ) -> Optional[Any]:
        # The next page is a cursor read from the response, so `page_size_override` does not change how the token
        # is computed. It is still exposed to the interpolation context as `page_size` because a `stop_condition`
        # comparing `last_page_size` to a hardcoded page size would read a full reduced page as a short page and
        # end the pagination early, silently dropping the rest of the partition. Writing the condition as
        # `{{ last_page_size < page_size }}` keeps it correct while a reduction is in effect.
        requested_page_size = (
            page_size_override if page_size_override is not None else self._page_size
        )
        decoded_response = next(self.decoder.decode(response))
        # The default way that link is presented in requests.Response is a string of various links (last, next, etc). This
        # is not indexable or useful for parsing the cursor, so we replace it with the link dictionary from response.links
        headers: Dict[str, Any] = dict(response.headers)
        headers["link"] = response.links
        if self._stop_condition:
            should_stop = self._stop_condition.eval(
                self.config,
                response=decoded_response,
                headers=headers,
                last_record=last_record,
                last_page_size=last_page_size,
                page_size=requested_page_size,
            )
            if should_stop:
                return None
        token = self._cursor_value.eval(
            config=self.config,
            response=decoded_response,
            headers=headers,
            last_record=last_record,
            last_page_size=last_page_size,
            page_size=requested_page_size,
        )
        return token if token else None

    def get_page_size(self) -> Optional[int]:
        return self._page_size
