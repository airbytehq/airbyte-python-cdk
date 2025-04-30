# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
"""Pytest hooks for Airbyte CDK tests.

These hooks are used to customize the behavior of pytest during test discovery and execution.

To use these hooks within a connector, add the following lines to the connector's `conftest.py`
file, or to another file that is imported during test discovery:

```python
pytest_plugins = [
    "airbyte_cdk.test.standard_tests.pytest_hooks",
]
```
"""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--use-docker-image",
        action="store",
        dest="use_docker_image",
        metavar="IMAGE",
        default=None,
        help="test connector containerized",
    )


@pytest.fixture
def use_docker_image(request):
    """True if pytest was invoked with --use-docker-image."""
    return request.config.getoption("use_docker_image")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """
    A helper for pytest_generate_tests hook.

    If a test method (in a class subclassed from our base class)
    declares an argument 'scenario', this function retrieves the
    'scenarios' attribute from the test class and parametrizes that
    test with the values from 'scenarios'.

    ## Usage

    ```python
    from airbyte_cdk.test.standard_tests.connector_base import (
        generate_tests,
        ConnectorTestSuiteBase,
    )

    def pytest_generate_tests(metafunc):
        generate_tests(metafunc)

    class TestMyConnector(ConnectorTestSuiteBase):
        ...

    ```
    """
    # Check if the test function requires an 'scenario' argument
    if "scenario" not in metafunc.fixturenames and "use_docker_image" not in metafunc.fixturenames:
        return None

    # Retrieve the test class
    test_class = metafunc.cls
    if test_class is None:
        return

    argnames: list[str] = []
    argvalues: list[object] = []

    if "scenario" in metafunc.fixturenames:
        # Get the 'scenarios' attribute from the class
        scenarios_attr = getattr(test_class, "get_scenarios", None)
        if scenarios_attr is None:
            raise ValueError(
                f"Test class {test_class} does not have a 'scenarios' attribute. "
                "Please define the 'scenarios' attribute in the test class."
            )

        scenarios = test_class.get_scenarios()
        ids = [str(scenario) for scenario in scenarios]

        # if "use_docker_image" not in metafunc.fixturenames:
        #     raise ValueError(
        #         "The 'use_docker_image' argument should be used in conjunction with 'scenario'. "
        #         f"Metafunc: {metafunc.function}, Test class: {test_class!s}"
        #     )

        metafunc.parametrize("scenario", scenarios, ids=ids)

    # if "use_docker_image" in metafunc.fixturenames:
    #     metafunc.parametrize(
    #         "use_docker_image",
    #         [metafunc.config.getoption("use_docker_image")],
    #     )
