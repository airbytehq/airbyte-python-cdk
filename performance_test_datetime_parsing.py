import statistics
import time
from datetime import datetime, timezone

from dateutil import parser
from whenever import Instant, OffsetDateTime

from airbyte_cdk.utils.datetime_helpers import ab_datetime_parse

# Test formats
test_formats = [
    "2023-03-14T15:09:26Z",  # ISO format with T delimiter and Z timezone
    "2023-03-14T15:09:26+00:00",  # ISO format with +00:00 timezone
    "2023-03-14T15:09:26.123456Z",  # ISO format with microseconds
    "2023-03-14T15:09:26-04:00",  # ISO format with non-UTC timezone
    "2023-03-14 15:09:26Z",  # Missing T delimiter
    "2023-03-14 15:09:26",  # Missing T delimiter and timezone
    "2023-03-14",  # Date-only format
    "14/03/2023 15:09:26",  # Different date format
    "2023/03/14T15:09:26Z",  # Non-standard date separator
]

# Number of iterations for each test
iterations = 1000

print("Performance Test: Datetime Parsing")
print("=" * 60)
print(f"Running {iterations} iterations for each format")
print("-" * 60)

results = {}

for dt_str in test_formats:
    print(f"\nFormat: {dt_str}")

    # Test whenever parsing
    whenever_times = []
    whenever_success = False

    # Try Instant.parse_common_iso
    try:
        # Warmup
        Instant.parse_common_iso(dt_str)

        for _ in range(iterations):
            start = time.perf_counter()
            Instant.parse_common_iso(dt_str)
            end = time.perf_counter()
            whenever_times.append((end - start) * 1000)

        whenever_success = True
        whenever_method = "Instant.parse_common_iso"
    except Exception:
        pass

    # If parse_common_iso failed, try parse_rfc3339
    if not whenever_success:
        try:
            # Warmup
            Instant.parse_rfc3339(dt_str)

            whenever_times = []
            for _ in range(iterations):
                start = time.perf_counter()
                Instant.parse_rfc3339(dt_str)
                end = time.perf_counter()
                whenever_times.append((end - start) * 1000)

            whenever_success = True
            whenever_method = "Instant.parse_rfc3339"
        except Exception:
            pass

    # If both Instant methods failed, try OffsetDateTime.parse_common_iso
    if not whenever_success:
        try:
            # Warmup
            OffsetDateTime.parse_common_iso(dt_str)

            whenever_times = []
            for _ in range(iterations):
                start = time.perf_counter()
                OffsetDateTime.parse_common_iso(dt_str)
                end = time.perf_counter()
                whenever_times.append((end - start) * 1000)

            whenever_success = True
            whenever_method = "OffsetDateTime.parse_common_iso"
        except Exception:
            whenever_method = "None"

    # Test dateutil parsing
    dateutil_times = []
    try:
        # Warmup
        parser.parse(dt_str)

        for _ in range(iterations):
            start = time.perf_counter()
            parser.parse(dt_str)
            end = time.perf_counter()
            dateutil_times.append((end - start) * 1000)

        dateutil_success = True
    except Exception:
        dateutil_success = False

    # Test ab_datetime_parse
    ab_times = []
    try:
        # Warmup
        ab_datetime_parse(dt_str)

        for _ in range(iterations):
            start = time.perf_counter()
            ab_datetime_parse(dt_str)
            end = time.perf_counter()
            ab_times.append((end - start) * 1000)

        ab_success = True
    except Exception:
        ab_success = False

    # Print results
    if whenever_success:
        whenever_avg = statistics.mean(whenever_times)
        whenever_min = min(whenever_times)
        whenever_max = max(whenever_times)
        print(
            f"  {whenever_method}: avg={whenever_avg:.3f}ms, min={whenever_min:.3f}ms, max={whenever_max:.3f}ms"
        )
    else:
        print(f"  whenever: Failed to parse")

    if dateutil_success:
        dateutil_avg = statistics.mean(dateutil_times)
        dateutil_min = min(dateutil_times)
        dateutil_max = max(dateutil_times)
        print(
            f"  dateutil.parser.parse: avg={dateutil_avg:.3f}ms, min={dateutil_min:.3f}ms, max={dateutil_max:.3f}ms"
        )
    else:
        print(f"  dateutil.parser.parse: Failed to parse")

    if ab_success:
        ab_avg = statistics.mean(ab_times)
        ab_min = min(ab_times)
        ab_max = max(ab_times)
        print(f"  ab_datetime_parse: avg={ab_avg:.3f}ms, min={ab_min:.3f}ms, max={ab_max:.3f}ms")
    else:
        print(f"  ab_datetime_parse: Failed to parse")

    # Compare performance if both succeeded
    if whenever_success and dateutil_success:
        speedup = dateutil_avg / whenever_avg
        print(f"  Performance: whenever is {speedup:.2f}x faster than dateutil")

    # Store results for summary
    results[dt_str] = {
        "whenever": {
            "success": whenever_success,
            "method": whenever_method,
            "avg": statistics.mean(whenever_times) if whenever_success else None,
        },
        "dateutil": {
            "success": dateutil_success,
            "avg": statistics.mean(dateutil_times) if dateutil_success else None,
        },
        "ab_datetime_parse": {
            "success": ab_success,
            "avg": statistics.mean(ab_times) if ab_success else None,
        },
    }

# Print summary
print("\n\nSummary")
print("=" * 60)
print("Format | whenever | dateutil | ab_datetime_parse | Speedup")
print("-" * 60)

for dt_str, result in results.items():
    whenever_avg = result["whenever"]["avg"]
    dateutil_avg = result["dateutil"]["avg"]
    ab_avg = result["ab_datetime_parse"]["avg"]

    whenever_str = f"{whenever_avg:.3f}ms" if whenever_avg else "N/A"
    dateutil_str = f"{dateutil_avg:.3f}ms" if dateutil_avg else "N/A"
    ab_str = f"{ab_avg:.3f}ms" if ab_avg else "N/A"

    if whenever_avg and dateutil_avg:
        speedup = dateutil_avg / whenever_avg
        speedup_str = f"{speedup:.2f}x"
    else:
        speedup_str = "N/A"

    print(f"{dt_str} | {whenever_str} | {dateutil_str} | {ab_str} | {speedup_str}")
