"""Custom Python components.py file for testing.

This file is mostly a no-op (for now) but should trigger a failure if code file is not
correctly parsed.
"""

from airbyte_cdk.sources.declarative.models import DeclarativeStream


class CustomDeclarativeStream(DeclarativeStream):
    """Custom declarative stream class.

    We don't change anything from the base class, but this should still be enough to confirm
    that the components.py file is correctly parsed.
    """
