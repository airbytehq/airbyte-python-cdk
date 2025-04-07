"""General fixtures for pytest.

Usage:

```python
from airbyte_cdk.test.fixtures.general import *
# OR:
from airbyte_cdk.test.fixtures.general import connector_test_dir
```
"""


@pytest.fixture
def connector_test_dir():
    return Path(__file__).parent
