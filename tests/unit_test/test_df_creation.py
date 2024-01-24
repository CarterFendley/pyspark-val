import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    FloatType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pyspark_val.create import df_from_dict
from pyspark_val.assertion import assert_dfs_equal

def test_simple(spark_session: SparkSession):
    expect = spark_session.createDataFrame(
        data=[
            [datetime.date(2020, 1, 1), "foo", 1.123, 10],
            [datetime.date(2021, 1, 1), "bar", 3.145, 42]
        ],
        schema=StructType(
            [
                StructField("col_a", DateType(), True),
                StructField("col_b", StringType(), True),
                StructField("col_c", DoubleType(), True),
                StructField("col_d", LongType(), True),
            ]
        ),
    )

    test = df_from_dict(
        spark_session,
        {
            'col_a': [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)],
            'col_b': ["foo", "bar"],
            'col_c': [1.123, 3.145],
            'col_d': [10, 42]
        }
    )

    assert_dfs_equal(test, expect)

def test_null(spark_session: SparkSession):
    expect = spark_session.createDataFrame(
        data=[
            [datetime.date(2020, 1, 1), "foo", None, 10],
            [datetime.date(2021, 1, 1), "bar", 3.145, None]
        ],
        schema=StructType(
            [
                StructField("col_a", DateType(), True),
                StructField("col_b", StringType(), True),
                StructField("col_c", DoubleType(), True),
                StructField("col_d", LongType(), True),
            ]
        ),
    )

    test = df_from_dict(
        spark_session,
        {
            'col_a': [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)],
            'col_b': ["foo", "bar"],
            'col_c': [None, 3.145],
            'col_d': [10, None]
        }
    )

    assert_dfs_equal(test, expect)

def test_with_schema(spark_session: SparkSession):
    expect = spark_session.createDataFrame(
        data=[
            [0, 0.0],
            [1, 1.0]
        ],
        schema=StructType([
            StructField('col_int', IntegerType(), True),
            StructField('col_float', FloatType(), True)
        ])
    )

    test = df_from_dict(
        spark=spark_session,
        data={
            'col_int': [0, 1],
            'col_float': [0.0, 1.0]
        },
        types={
            # NOTE: PySpark will default to bigint, double
            'col_int': IntegerType(),
            'col_float': FloatType()
        }
    )

    assert_dfs_equal(test, expect)