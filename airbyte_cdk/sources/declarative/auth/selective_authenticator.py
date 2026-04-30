#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from dataclasses import dataclass
from typing import Any, List, Mapping

import dpath

from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.declarative.auth.declarative_authenticator import DeclarativeAuthenticator
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


@dataclass
class SelectiveAuthenticator(DeclarativeAuthenticator):
    """Authenticator that selects concrete implementation based on specific config value."""

    config: Mapping[str, Any]
    authenticators: Mapping[str, DeclarativeAuthenticator]
    authenticator_selection_path: List[str]

    # returns "DeclarativeAuthenticator", but must return a subtype of "SelectiveAuthenticator"
    def __new__(  # type: ignore[misc]
        cls,
        config: Mapping[str, Any],
        authenticators: Mapping[str, DeclarativeAuthenticator],
        authenticator_selection_path: List[str],
        *arg: Any,
        **kwargs: Any,
    ) -> DeclarativeAuthenticator:
        dotted_path = ".".join(authenticator_selection_path)
        try:
            selected_key = str(
                dpath.get(
                    config,  # type: ignore[arg-type]  # Dpath wants mutable mapping but doesn't need it.
                    authenticator_selection_path,
                )
            )
        except KeyError as err:
            raise AirbyteTracedException(
                message=f'Required field "{dotted_path}" is missing from connector configuration.',
                internal_message=f"SelectiveAuthenticator could not find the path {authenticator_selection_path} in the config. Available top-level config keys: {list(config.keys())}",
                failure_type=FailureType.config_error,
            ) from err

        if selected_key not in authenticators:
            raise AirbyteTracedException(
                message=f'Configuration field "{dotted_path}" contains unrecognized value "{selected_key}".',
                internal_message=f'SelectiveAuthenticator received key "{selected_key}" but available authenticators are: {list(authenticators.keys())}',
                failure_type=FailureType.config_error,
            )
        return authenticators[selected_key]
