# Testing Connectors with Mock HTTP Responses

This guide explains how to write effective unit tests for your Airbyte connectors using the CDK's mock HTTP testing utilities.

## Overview

The Airbyte Python CDK provides `HttpMocker`, a powerful testing utility that intercepts HTTP requests and returns predefined responses. This allows you to test connector logic without making actual API calls, making tests faster, more reliable, and independent of external services.

## Quick Start

Here's a minimal example of testing a connector stream:

```python
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequest, HttpResponse
from airbyte_cdk.test.entrypoint_wrapper import read
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.models import SyncMode

@HttpMocker()
def test_basic_read(http_mocker):
    # Mock the HTTP request
    http_mocker.get(
        HttpRequest("https://api.example.com/v1/users"),
        HttpResponse('{"users": [{"id": 1, "name": "Alice"}]}', 200)
    )
    
    # Configure and run the sync
    source = MySource()
    catalog = CatalogBuilder().with_stream("users", SyncMode.full_refresh).build()
    output = read(source, config={}, catalog=catalog)
    
    # Verify results
    assert len(output.records) == 1
    assert output.records[0].record.data["name"] == "Alice"
```

## Core Components

### HttpMocker

`HttpMocker` is a context manager and decorator that intercepts HTTP requests made by the `requests` library. Use it to define expected requests and their responses.

**As a decorator:**
```python
@HttpMocker()
def test_my_stream(http_mocker):
    http_mocker.get(...)
```

**As a context manager:**
```python
def setUp(self):
    self._http_mocker = HttpMocker()
    self._http_mocker.__enter__()

def tearDown(self):
    self._http_mocker.__exit__(None, None, None)
```

### HttpRequest

Defines the expected HTTP request to match. You can specify URL, query parameters, headers, and body.

```python
# Basic request
request = HttpRequest("https://api.example.com/users")

# With query parameters
request = HttpRequest(
    "https://api.example.com/users",
    query_params={"page": "1", "limit": "100"}
)

# With headers
request = HttpRequest(
    "https://api.example.com/users",
    headers={"Authorization": "Bearer token123"}
)
```

### HttpResponse

Defines the mocked response to return. You can specify body (string or bytes), status code, and headers.

```python
# JSON response
response = HttpResponse('{"data": []}', 200)

# Error response
response = HttpResponse('{"error": "Not found"}', 404)

# With custom headers
response = HttpResponse(
    '{"data": []}',
    200,
    headers={"X-RateLimit-Remaining": "99"}
)
```

### Response Builders

For complex responses, use the builder pattern to construct structured test data:

```python
from airbyte_cdk.test.mock_http.response_builder import (
    create_response_builder,
    create_record_builder,
    FieldPath,
    FieldUpdatePaginationStrategy,
)

# Define response template
RESPONSE_TEMPLATE = {
    "data": [],
    "has_more": False,
    "next_cursor": None
}

# Create response builder
response_builder = create_response_builder(
    response_template=RESPONSE_TEMPLATE,
    records_path=FieldPath("data"),
    pagination_strategy=FieldUpdatePaginationStrategy(
        FieldPath("has_more"), True
    )
)

# Create record builder
record_builder = create_record_builder(
    response_template=RESPONSE_TEMPLATE,
    records_path=FieldPath("data"),
    record_id_path=FieldPath("id"),
    record_cursor_path=FieldPath("updated_at")
)

# Build response with records
http_mocker.get(
    request,
    response_builder
        .with_record(record_builder.with_id("123").with_cursor("2024-01-01"))
        .with_record(record_builder.with_id("456").with_cursor("2024-01-02"))
        .with_pagination()
        .build()
)
```

## Testing Patterns

### Full Refresh Streams

Test that streams correctly fetch all records in full refresh mode:

```python
@HttpMocker()
def test_full_refresh_sync(http_mocker):
    http_mocker.get(
        HttpRequest("https://api.example.com/users"),
        HttpResponse('{"users": [{"id": 1}, {"id": 2}]}', 200)
    )
    
    source = MySource()
    catalog = CatalogBuilder().with_stream("users", SyncMode.full_refresh).build()
    output = read(source, config={}, catalog=catalog)
    
    assert len(output.records) == 2
```

### Incremental Streams

Test that streams correctly handle state and incremental syncs:

```python
from airbyte_cdk.test.state_builder import StateBuilder

@HttpMocker()
def test_incremental_sync(http_mocker):
    # First sync - no state
    http_mocker.get(
        HttpRequest(
            "https://api.example.com/users",
            query_params={"updated_since": "2024-01-01T00:00:00Z"}
        ),
        HttpResponse('{"users": [{"id": 1, "updated_at": "2024-01-15"}]}', 200)
    )
    
    source = MySource()
    config = {"start_date": "2024-01-01T00:00:00Z"}
    catalog = CatalogBuilder().with_stream("users", SyncMode.incremental).build()
    output = read(source, config=config, catalog=catalog)
    
    # Verify state is emitted
    assert len(output.state_messages) == 1
    assert output.state_messages[0].state.stream.stream_state.updated_at == "2024-01-15"
```

### Pagination

Test multi-page responses:

```python
@HttpMocker()
def test_pagination(http_mocker):
    # First page
    http_mocker.get(
        HttpRequest("https://api.example.com/users"),
        HttpResponse('{"users": [{"id": 1}], "next_page": "page2"}', 200)
    )
    
    # Second page
    http_mocker.get(
        HttpRequest("https://api.example.com/users", query_params={"page": "page2"}),
        HttpResponse('{"users": [{"id": 2}], "next_page": null}', 200)
    )
    
    output = read(source, config={}, catalog=catalog)
    assert len(output.records) == 2
```

### Error Handling and Retries

Test that your connector properly handles errors and retries:

```python
@HttpMocker()
def test_retry_on_rate_limit(http_mocker):
    # Return rate limit error, then success
    http_mocker.get(
        HttpRequest("https://api.example.com/users"),
        [
            HttpResponse('{"error": "Rate limit exceeded"}', 429),
            HttpResponse('{"users": [{"id": 1}]}', 200)
        ]
    )
    
    output = read(source, config={}, catalog=catalog)
    assert len(output.records) == 1
```

### Authentication

Test OAuth and other authentication flows:

```python
@HttpMocker()
def test_oauth_authentication(http_mocker):
    # Mock token refresh
    http_mocker.post(
        HttpRequest(
            "https://oauth.example.com/token",
            body='grant_type=refresh_token&refresh_token=refresh123'
        ),
        HttpResponse('{"access_token": "new_token", "expires_in": 3600}', 200)
    )
    
    # Mock API call with new token
    http_mocker.get(
        HttpRequest(
            "https://api.example.com/users",
            headers={"Authorization": "Bearer new_token"}
        ),
        HttpResponse('{"users": []}', 200)
    )
```

### Parent-Child Stream Relationships

Test substreams that depend on parent stream data:

```python
@HttpMocker()
def test_substream(http_mocker):
    # Parent stream request
    http_mocker.get(
        HttpRequest("https://api.example.com/users"),
        HttpResponse('{"users": [{"id": "user1"}, {"id": "user2"}]}', 200)
    )
    
    # Child stream requests for each parent record
    http_mocker.get(
        HttpRequest("https://api.example.com/users/user1/posts"),
        HttpResponse('{"posts": [{"id": "post1"}]}', 200)
    )
    
    http_mocker.get(
        HttpRequest("https://api.example.com/users/user2/posts"),
        HttpResponse('{"posts": [{"id": "post2"}]}', 200)
    )
    
    catalog = CatalogBuilder().with_stream("user_posts", SyncMode.full_refresh).build()
    output = read(source, config={}, catalog=catalog)
    assert len(output.records) == 2
```

## Advanced Techniques

### Request Builders

Create helper functions to build requests consistently:

```python
class RequestBuilder:
    @classmethod
    def users_endpoint(cls):
        return cls("users")
    
    def __init__(self, resource):
        self._resource = resource
        self._params = {}
    
    def with_page(self, page):
        self._params["page"] = page
        return self
    
    def build(self):
        return HttpRequest(
            f"https://api.example.com/{self._resource}",
            query_params=self._params
        )

# Usage
request = RequestBuilder.users_endpoint().with_page(2).build()
```

### Freezing Time

Use `freezegun` to test time-dependent behavior:

```python
import freezegun
from datetime import datetime, timezone

@freezegun.freeze_time("2024-01-15T12:00:00Z")
@HttpMocker()
def test_with_frozen_time(http_mocker):
    now = datetime.now(timezone.utc)  # Always returns 2024-01-15T12:00:00Z
    # Test time-dependent logic
```

### Asserting Request Count

Verify that expected requests were made the correct number of times:

```python
@HttpMocker()
def test_request_count(http_mocker):
    request = HttpRequest("https://api.example.com/users")
    http_mocker.get(request, HttpResponse('{"users": []}', 200))
    
    # Perform sync
    read(source, config={}, catalog=catalog)
    
    # Assert request was called exactly once
    http_mocker.assert_number_of_calls(request, 1)
```

### Testing Stream Status Messages

Verify that streams emit correct status messages:

```python
from airbyte_cdk.models import AirbyteStreamStatus

@HttpMocker()
def test_stream_status(http_mocker):
    http_mocker.get(request, response)
    
    output = read(source, config={}, catalog=catalog)
    statuses = output.get_stream_statuses("users")
    
    assert statuses[0] == AirbyteStreamStatus.STARTED
    assert statuses[1] == AirbyteStreamStatus.RUNNING
    assert statuses[2] == AirbyteStreamStatus.COMPLETE
```

## Real-World Examples

### Example from Salesforce Connector

This example shows testing incremental sync with state migration:

```python
@freezegun.freeze_time(_NOW.isoformat())
class IncrementalTest(TestCase):
    def setUp(self):
        self._config = ConfigBuilder().client_id(_CLIENT_ID).client_secret(_CLIENT_SECRET)
        self._http_mocker = HttpMocker()
        self._http_mocker.__enter__()
        
        # Mock authentication
        self._http_mocker.post(
            HttpRequest("https://login.salesforce.com/services/oauth2/token"),
            HttpResponse('{"access_token": "token123"}', 200)
        )
    
    def tearDown(self):
        self._http_mocker.__exit__(None, None, None)
    
    def test_given_sequential_state_when_read_then_migrate_to_partitioned_state(self):
        cursor_value = _NOW - timedelta(days=5)
        
        self._http_mocker.get(
            HttpRequest(
                "https://instance.salesforce.com/services/data/v57.0/queryAll",
                query_params={"q": f"SELECT * FROM Account WHERE SystemModstamp >= {cursor_value}"}
            ),
            HttpResponse('{"records": [{"Id": "123"}]}', 200)
        )
        
        output = read(
            "accounts",
            SyncMode.incremental,
            self._config,
            StateBuilder().with_stream_state("accounts", {"SystemModstamp": cursor_value})
        )
        
        # Verify state was migrated to new format
        assert output.most_recent_state.stream_state["state_type"] == "date-range"
```

### Example from Hubspot Connector

This example shows testing resumable full refresh with pagination:

```python
@HttpMocker()
def test_read_multiple_contact_pages(http_mocker):
    # First page
    http_mocker.get(
        ContactsRequestBuilder().build(),
        ContactsResponseBuilder()
            .with_pagination(vid_offset=5331889818)
            .with_contacts([
                ContactBuilder().with_id("c1"),
                ContactBuilder().with_id("c2")
            ])
            .build()
    )
    
    # Second page
    http_mocker.get(
        ContactsRequestBuilder().with_vid_offset("5331889818").build(),
        ContactsResponseBuilder()
            .with_contacts([
                ContactBuilder().with_id("c3")
            ])
            .build()
    )
    
    output = read(source, config, catalog)
    
    assert len(output.records) == 3
    assert output.state_messages[0].state.stream.stream_state == {"vidOffset": 5331889818}
```

## Best Practices

1. **Test in Isolation**: Each test should be independent and not rely on the order of execution.

2. **Use Builders**: For complex requests and responses, create builder classes to keep tests readable.

3. **Test Edge Cases**: Include tests for empty responses, errors, rate limits, and boundary conditions.

4. **Mock Realistically**: Match the actual API response structure and behavior as closely as possible.

5. **Verify State**: For incremental streams, always verify that state is correctly emitted and updated.

6. **Test All Sync Modes**: Ensure both full_refresh and incremental modes work correctly.

7. **Use Descriptive Names**: Test method names should clearly describe what scenario is being tested.

8. **Setup and Teardown**: Use `setUp()` and `tearDown()` methods to initialize and clean up `HttpMocker` when testing multiple scenarios in a test class.

## Troubleshooting

### "No matcher matches" Error

This means an HTTP request was made that doesn't match any mocked request. Check:
- URL matches exactly (including protocol)
- Query parameters match
- Headers match (if specified)
- Request method (GET, POST, etc.) is correct

### "Invalid number of matches" Error

This means a mocked request was called a different number of times than expected. The mocker expects each request to be called exactly as many times as responses are provided.

### Request Not Being Mocked

Ensure:
- You're using the `@HttpMocker()` decorator or context manager
- The request is made using the `requests` library (HttpMocker only works with `requests`)
- The request matcher is defined before the code that makes the request

## Additional Resources

- [CDK Test Utilities API Reference](../airbyte_cdk/test/)
- [Example Tests in CDK](../../unit_tests/sources/mock_server_tests/)
- [Salesforce Connector Tests](https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-salesforce/unit_tests/integration/)
- [Hubspot Connector Tests](https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-hubspot/unit_tests/integrations/)
