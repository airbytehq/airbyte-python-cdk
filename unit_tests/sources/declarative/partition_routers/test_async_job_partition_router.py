# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from airbyte_cdk.sources.declarative.async_job.job_orchestrator import (
    AsyncJobOrchestrator,
    AsyncPartition,
)
from airbyte_cdk.sources.declarative.async_job.job_tracker import JobTracker
from airbyte_cdk.sources.declarative.async_job.status import AsyncJobStatus
from airbyte_cdk.sources.declarative.partition_routers import ListPartitionRouter
from airbyte_cdk.sources.declarative.partition_routers.async_job_partition_router import (
    AsyncJobPartitionRouter,
)
from airbyte_cdk.sources.declarative.partition_routers.single_partition_router import (
    SinglePartitionRouter,
)
from airbyte_cdk.sources.message import NoopMessageRepository
from airbyte_cdk.sources.types import StreamSlice
from unit_tests.sources.declarative.async_job.test_integration import MockAsyncJobRepository

_NO_LIMIT = 10000


def test_stream_slices_with_single_partition_router():
    partition_router = AsyncJobPartitionRouter(
        stream_slicer=SinglePartitionRouter(parameters={}),
        job_orchestrator_factory=lambda stream_slices: AsyncJobOrchestrator(
            MockAsyncJobRepository(),
            stream_slices,
            JobTracker(_NO_LIMIT),
            NoopMessageRepository(),
        ),
        config={},
        parameters={},
    )

    slices = list(partition_router.stream_slices())
    assert len(slices) == 1
    partition = slices[0]
    assert isinstance(partition, StreamSlice)
    assert partition == StreamSlice(partition={}, cursor_slice={})
    assert partition.extra_fields["jobs"][0].status() == AsyncJobStatus.COMPLETED

    attempts_per_job = list(partition.extra_fields["jobs"])
    assert len(attempts_per_job) == 1
    assert attempts_per_job[0].api_job_id() == "a_job_id"
    assert attempts_per_job[0].job_parameters() == StreamSlice(partition={}, cursor_slice={})
    assert attempts_per_job[0].status() == AsyncJobStatus.COMPLETED


def test_stream_slices_with_parent_slicer():
    partition_router = AsyncJobPartitionRouter(
        stream_slicer=ListPartitionRouter(
            values=["0", "1", "2"],
            cursor_field="parent_id",
            config={},
            parameters={},
        ),
        job_orchestrator_factory=lambda stream_slices: AsyncJobOrchestrator(
            MockAsyncJobRepository(),
            stream_slices,
            JobTracker(_NO_LIMIT),
            NoopMessageRepository(),
        ),
        config={},
        parameters={},
    )

    slices = list(partition_router.stream_slices())
    assert len(slices) == 3
    for i, partition in enumerate(slices):
        assert isinstance(partition, StreamSlice)
        assert partition == StreamSlice(partition={"parent_id": str(i)}, cursor_slice={})

        attempts_per_job = list(partition.extra_fields["jobs"])
        assert len(attempts_per_job) == 1
        assert attempts_per_job[0].api_job_id() == "a_job_id"
        assert attempts_per_job[0].job_parameters() == StreamSlice(
            partition={"parent_id": str(i)}, cursor_slice={}
        )
        assert attempts_per_job[0].status() == AsyncJobStatus.COMPLETED
