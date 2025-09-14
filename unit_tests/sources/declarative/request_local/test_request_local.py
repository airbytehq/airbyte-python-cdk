import sys
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor


from airbyte_cdk.sources.declarative.request_local.request_local import RequestLocal

STREAM_SLICE_KEY = "stream_slice"
INSTANCE_ID_KEY = "instance_id"

def test_basic_singleton():
    """Test basic singleton behavior"""
    # Multiple instantiations return same instance
    instance1 = RequestLocal()
    instance2 = RequestLocal()
    instance3 = RequestLocal()

    assert instance1 is instance2
    assert instance1 is instance3, "All instances should be the same singleton instance"
    assert instance2 is instance3, "All instances should be the same singleton instance"


    # get_instance class method
    instance4 = RequestLocal.get_instance()
    instance1.stream_slice = {"test": "data"}

    # stream_slice property
    instance1.stream_slice = {"test": "data"}
    assert instance1.stream_slice is instance4.stream_slice
    assert instance2.stream_slice is instance4.stream_slice

    return instance1


def create_instance_in_thread(thread_id, results):
    """Function to create instance in a separate thread"""
    instance = RequestLocal()

    results[thread_id] = {
        'instance_id': id(instance),
        'thread_id': threading.get_ident()
    }
    time.sleep(0.1)  # Small delay to ensure threads overlap


def test_thread_safety():
    """Ensure that RequestLocal is thread-safe and behaves as a singleton across threads"""
    print("\n=== Testing Thread Safety ===")

    results = {}
    threads = []
    total_treads = 5
    # Create multiple threads that instantiate RequestLocal
    for i in range(total_treads):
        thread = threading.Thread(target=create_instance_in_thread, args=(i, results))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Analyze results
    instance_ids = [result[INSTANCE_ID_KEY] for result in results.values()]
    unique_ids = set(instance_ids)

    assert len(results) == total_treads, "All threads should have created an instance"
    assert len(unique_ids) == 1, "All threads should see the same singleton instance"



def test_threading_local_behavior():
    """Test how threading.local affects the singleton"""
    def thread_func(thread_name, shared_results, time_sleep):
        instance = RequestLocal()
        assert instance.stream_slice == None, "Initial stream_slice should be empty"
        instance.stream_slice = {f"data_from_{thread_name}": True}

        shared_results[thread_name] = {
            'instance_id': id(instance),
            'stream_slice': instance.stream_slice.copy(),
            'thread_id': threading.get_ident()
        }

        # Check if we can see data from other threads
        # this should not happen as RequestLocal is a singleton
        time.sleep(time_sleep)
        shared_results[f"{thread_name}_after_sleep"] = {
            'instance_id': id(instance),
            'stream_slice': instance.stream_slice.copy(),
            'end_time': time.time(),
        }
    
    results = {}
    threads = {}
    threads_amount = 3
    time_sleep = 0.9
    thread_names = []
    for i in range(threads_amount):
        tread_name = f"thread_{i}"
        thread_names.append(tread_name)
        thread = threading.Thread(target=thread_func, args=(tread_name, results, time_sleep))
        time_sleep /=3  # Decrease sleep time for each thread to ensure they overlap
        threads[tread_name]= thread
        thread.start()

    for _, thread in threads.items():
        thread.join()

    end_times = [results[thread_name + "_after_sleep"]['end_time'] for thread_name in thread_names]
    last_end_time = end_times.pop()
    while end_times:
        current_end_time = end_times.pop()
        # Just checking the last thread created ended before the previous ones
        # so we could ensure the first thread created that sleep for a longer time
        # was not affected by the other threads
        assert last_end_time < current_end_time, "End times should be in increasing order"
        last_end_time = current_end_time

    assert len(thread_names) > 1
    assert len(set(thread_names)) == len(thread_names), "Thread names should be unique"
    for curren_thread_name in thread_names:
        current_thread_name_after_sleep = f"{curren_thread_name}_after_sleep"
        assert results[curren_thread_name][STREAM_SLICE_KEY] == results[current_thread_name_after_sleep][STREAM_SLICE_KEY], \
            f"Stream slice should remain consistent across thread {curren_thread_name} before and after sleep"
        assert results[curren_thread_name][INSTANCE_ID_KEY] == results[current_thread_name_after_sleep][INSTANCE_ID_KEY], \
            f"Instance ID should remain consistent across thread {curren_thread_name} before and after sleep"

        # Check if stream slices are different across threads
        # but same instance ID
        for other_tread_name in [thread_name for thread_name in thread_names if thread_name != curren_thread_name]:
            assert results[curren_thread_name][STREAM_SLICE_KEY] != results[other_tread_name][STREAM_SLICE_KEY], \
                f"Stream slices from different threads should not be the same: {curren_thread_name} vs {other_tread_name}"
            assert results[curren_thread_name][INSTANCE_ID_KEY] == results[other_tread_name][INSTANCE_ID_KEY]

# Fixme: Uncomment this test put asserts and remove prints to test concurrent access
# def test_concurrent_access():
#     """Test concurrent access using ThreadPoolExecutor"""
#     print("\n=== Testing Concurrent Access ===")
#
#     def worker(worker_id):
#         instance = RequestLocal()
#         return {
#             'worker_id': worker_id,
#             'instance_id': id(instance),
#             'thread_id': threading.get_ident()
#         }
#
#     with ThreadPoolExecutor(max_workers=10) as executor:
#         futures = [executor.submit(worker, i) for i in range(20)]
#         results = [future.result() for future in futures]
#
#     # Analyze results
#     instance_ids = [result[INSTANCE_ID_KEY] for result in results]
#     unique_ids = set(instance_ids)
#
#     print(f"Total workers: {len(results)}")
#     print(f"Unique instance IDs: {len(unique_ids)}")
#     print(f"Singleton behavior maintained: {len(unique_ids) == 1}")
#
#     # Show first few results
#     print("First 5 results:")
#     for result in results[:5]:
#         print(f"  Worker {result['worker_id']}: ID={result[INSTANCE_ID_KEY]}")

