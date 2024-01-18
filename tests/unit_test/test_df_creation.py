import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pyspark_val import from_dict, assert_pyspark_df_equal

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

    test = from_dict(
        spark_session,
        {
            'col_a': [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)],
            'col_b': ["foo", "bar"],
            'col_c': [1.123, 3.145],
            'col_d': [10, 42]
        }
    )

    assert_pyspark_df_equal(test, expect)