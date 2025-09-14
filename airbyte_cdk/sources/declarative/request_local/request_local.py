from threading import local, Lock

class RequestLocal(local):
    _instance = None
    _lock = Lock()  # Thread-safe singleton creation

    def __new__(cls, *args, **kwargs):
        # Use double-checked locking for thread safety
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(RequestLocal, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # __init__ will be called every time the class is instantiated,
        # but the object itself is only created once by __new__.
        # Use a flag to prevent re-initialization
        if not hasattr(self, '_initialized'):
            self._stream_slice = None  # Initialize _stream_slice
            self._initialized = True

    @property
    def stream_slice(self):
        return self._stream_slice

    @stream_slice.setter
    def stream_slice(self, stream_slice):
        self._stream_slice = stream_slice

    @classmethod
    def get_instance(cls):
        """
        Get the singleton instance of RequestLocal.
        This is the recommended way to get the instance.
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance