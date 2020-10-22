import pytest

import bionic as bn

# This is detected by pytest and applied to all the tests in this module.
pytestmark = pytest.mark.needs_aip


def test_aip_jobs(aip_builder):
    builder = aip_builder

    builder.assign("a", 1)

    # Test various combinations of memoize and persist settings for these
    # function entities.

    @builder
    def b():
        return 2

    @builder
    @bn.persist(False)
    def c():
        return 3

    @builder
    @bn.memoize(False)
    def d():
        return 4

    @builder
    @bn.persist(False)
    @bn.memoize(False)
    def e():
        return 5

    @builder
    @bn.aip_task_config("n1-standard-4")
    def x(a, b, c, d, e):
        return a + b + c + d + e + 1

    @builder
    @bn.aip_task_config("n1-standard-8")
    def y(a, b, c, d, e):
        return a + b + c + d + e + 1

    @builder
    def total(x, y):
        return x + y

    assert builder.build().get("total") == 32
