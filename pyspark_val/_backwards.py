from functools import wraps

from .assertion import assert_dfs_equal

# Thin wrapper to maintain backwards compatibility with pyspark-test
@wraps(assert_dfs_equal)
def assert_pyspark_df_equal(*args, **kwargs):
    return assert_dfs_equal(*args, **kwargs)