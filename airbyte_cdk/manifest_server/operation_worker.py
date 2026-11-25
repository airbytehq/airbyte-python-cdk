"""Private entry point for one supervised Manifest Server operation."""

import logging
import os
import sys
from typing import cast

from fastapi import HTTPException

from airbyte_cdk.logger import AirbyteLogFormatter
from airbyte_cdk.utils.airbyte_secrets_utils import filter_secrets

# Import the application to install the same outbound policy as the HTTP process.
from .app import app  # noqa: F401
from .execution import _MAX_MESSAGE_BYTES, WorkerInput, WorkerResult, operation_deadline
from .helpers.security_events import WorkerOperation, bind_worker_operation
from .routers.manifest import WORKER_OPERATIONS

logger = logging.getLogger("airbyte.manifest-server")


def main(operation: WorkerOperation, result_fd: int, deadline: float) -> None:
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(AirbyteLogFormatter())
    logging.basicConfig(handlers=[log_handler], level=logging.INFO, force=True)

    operation_deadline.set(deadline)
    request_type, handler = WORKER_OPERATIONS[operation]
    try:
        worker_input = WorkerInput.read(sys.stdin.buffer)
        request = request_type.model_validate_json(worker_input.payload)
        with bind_worker_operation(operation, worker_input.builder_request_id):
            response = handler(request)
        result = WorkerResult(status=200, payload=response.model_dump_json())
    except HTTPException as exc:
        result = WorkerResult(status=exc.status_code, payload=filter_secrets(str(exc.detail)))
    except Exception:
        logger.exception("Unexpected exception in Manifest worker for %s", operation)
        result = WorkerResult(status=500, payload="Manifest operation failed to complete.")

    encoded = result.model_dump_json().encode()
    if len(encoded) > _MAX_MESSAGE_BYTES:
        encoded = (
            WorkerResult(status=400, payload="Manifest operation result is too large.")
            .model_dump_json()
            .encode()
        )
    with os.fdopen(result_fd, "wb") as result_file:
        result_file.write(encoded)


if __name__ == "__main__":
    main(cast(WorkerOperation, sys.argv[1]), int(sys.argv[2]), float(sys.argv[3]))
