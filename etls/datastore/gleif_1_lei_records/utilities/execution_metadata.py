# utilities/execution_metadata.py

class ExecutionMetadata:
    """Holds metadata about the current ETL execution."""

    def __init__(self):
        self.data = {}

    def set(self, key, value):
        self.data[key] = value

    def get(self, key, default=None):
        return self.data.get(key, default)
