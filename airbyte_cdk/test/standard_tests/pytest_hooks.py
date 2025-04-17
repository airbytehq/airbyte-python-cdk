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
    if "scenario" in metafunc.fixturenames:
        # Retrieve the test class
        test_class = metafunc.cls
        if test_class is None:
            return

        # Get the 'scenarios' attribute from the class
        scenarios_attr = getattr(test_class, "get_scenarios", None)
        if scenarios_attr is None:
            raise ValueError(
                f"Test class {test_class} does not have a 'scenarios' attribute. "
                "Please define the 'scenarios' attribute in the test class."
            )

        scenarios = test_class.get_scenarios()
        ids = [str(scenario) for scenario in scenarios]
        metafunc.parametrize("scenario", scenarios, ids=ids)
