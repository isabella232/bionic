import pytest

from ..conftest import ExecutionMode

from bionic.executor import get_singleton_manager, logging_initializer
from bionic.optdep import import_optional_dependency


# TODO use a marker to run parallel execution mode tests.
# This overrides the fixture defined in conftest.py.
@pytest.fixture(params=[ExecutionMode.PARALLEL])
def parallel_processing_enabled(request):
    return request.param == ExecutionMode.PARALLEL


@pytest.fixture
def loky_executor():
    loky = import_optional_dependency("loky", purpose="parallel processing")
    return loky.get_reusable_executor(
        max_workers=None,
        initializer=logging_initializer,
        initargs=(get_singleton_manager().logging_queue,),
    )


def test_executor_resizes(builder, loky_executor):
    builder.assign("x", 1)

    @builder
    def y(x):
        return x

    @builder
    def z(x):
        return x

    builder.set("core__parallel_processing__worker_count", 2)
    flow1 = builder.build()

    builder.set("core__parallel_processing__worker_count", 3)
    flow2 = builder.build()

    assert flow1.get("y") == 1
    # It's gross to check a private variable of the executor but this is
    # the best way to check that it was resized correctly.
    # TODO: Return PIDs in functions and assert that PIDs are different.
    assert loky_executor._max_workers == 2

    assert flow2.get("y") == 1
    assert loky_executor._max_workers == 3

    # Call a non-cached entity so that a task is submitted to executor
    # and it resizes.
    assert flow1.get("z") == 1
    assert loky_executor._max_workers == 2