from typing import Dict, Any
from unittest.mock import Mock

from pyspark.sql.readwriter import PathOrPaths

def mock_read_parquet(paths: Dict[str, Any], mock: Mock = None) -> Mock:
    return mock_read('parquet', paths, mock)

def mock_read(type: str, paths: Dict[str, Any], mock: Mock = None) -> Mock:
    if mock is None:
        mock = Mock()

    def side_effect(path: str, *args, **kwargs):
        nonlocal paths

        if path not in paths:
            raise RuntimeError(f"Path not found in the mock's 'paths' dict: {path}")

        return paths[path]

    # Apply side effect to mock
    read_type = getattr(mock.read, type)
    read_type.side_effect = side_effect

    return mock