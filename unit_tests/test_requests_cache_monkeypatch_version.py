import requests_cache


def test_assert_requests_cache_version():
    """
    We need to be alerted once the requests_cache version is updated. The reason is that we monkey patch one of the
    method in order to fix a bug until a new version is released.

    For more information about the reasons of this test, see monkey_patched_get_item in http_client.py
    """
    assert requests_cache.__version__ == "1.2.1"
