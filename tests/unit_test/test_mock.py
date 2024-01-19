from unittest.mock import Mock

import pytest


from pyspark_val.mock import mock_read

def test_mock_read():
    paths = {
        'my/path/to/file_one.parquet': '_file_one_',
        'my/path/to/file_two.parquet': '_file_two_',
        'my/path/to/file_three.parquet': '_file_three_',
        'my/path/to/file_four.parquet': '_file_four_',
    }
    spark = mock_read('parquet', paths)

    for key, value in paths.items():
        assert spark.read.parquet(key) == value

    with pytest.raises(
        RuntimeError,
        match="Path not found in the mock's 'paths' dict: does/not/exist.parquet"
    ):
        spark.read.parquet('does/not/exist.parquet')

def test_existing_mock_read():
    paths = {
        'my/path/to/file_one.parquet': '_file_one_',
        'my/path/to/file_two.parquet': '_file_two_',
        'my/path/to/file_three.parquet': '_file_three_',
        'my/path/to/file_four.parquet': '_file_four_',
    }
    spark = Mock()
    mock_read('parquet', paths, spark)

    for key, value in paths.items():
        assert spark.read.parquet(key) == value