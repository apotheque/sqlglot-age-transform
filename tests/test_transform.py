import sqlglot
from sqlglot_age_transform.transform import transform_age
import pytest

import duckdb
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("sqlglot_age_tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def assert_age_exec_equal(sql: str, transformed_sql: str, spark_session):
    duck_interval = duckdb.sql(
        "SELECT "
        "EXTRACT(YEAR FROM age) AS y, "
        "EXTRACT(MONTH FROM age) AS m, "
        "EXTRACT(DAY FROM age) AS d "
        f"FROM ({sql}) t"
    ).fetchone()

    spark_interval_query = (
        "SELECT "
        "EXTRACT(YEAR FROM age) AS y, "
        "EXTRACT(MONTH FROM age) AS m, "
        "EXTRACT(DAY FROM age) AS d "
        f"FROM ({transformed_sql}) t"
    )

    df = spark_session.sql(spark_interval_query)
    spark_interval = tuple(df.collect()[0])

    assert spark_interval == duck_interval


@pytest.mark.parametrize(
    "sql, expected_sql, exec_checker",
    [
        (
            "SELECT AGE('2025-12-02'::DATE, '2024-11-01'::DATE) AS age",
            "SELECT MAKE_INTERVAL("
            "CAST(MONTHS_BETWEEN(CAST('2025-12-02' AS DATE), CAST('2024-11-01' AS DATE)) AS INT) / 12, "
            "CAST(MONTHS_BETWEEN(CAST('2025-12-02' AS DATE), CAST('2024-11-01' AS DATE)) AS INT) % 12, 0, "
            "DATEDIFF(CAST('2025-12-02' AS DATE), "
            "ADD_MONTHS(CAST('2024-11-01' AS DATE), "
            "CAST(MONTHS_BETWEEN(CAST('2025-12-02' AS DATE), CAST('2024-11-01' AS DATE)) AS INT))), "
            "0, 0, 0) AS age",
            assert_age_exec_equal,
        ),
        (
            "SELECT AGE('2025-12-02'::DATE)",
            "SELECT MAKE_INTERVAL("
            "CAST(MONTHS_BETWEEN(CURRENT_TIMESTAMP(), CAST('2025-12-02' AS DATE)) AS INT) / 12, "
            "CAST(MONTHS_BETWEEN(CURRENT_TIMESTAMP(), CAST('2025-12-02' AS DATE)) AS INT) % 12, 0, "
            "DATEDIFF(CURRENT_TIMESTAMP(), "
            "ADD_MONTHS(CAST('2025-12-02' AS DATE), "
            "CAST(MONTHS_BETWEEN(CURRENT_TIMESTAMP(), CAST('2025-12-02' AS DATE)) AS INT))), "
            "0, 0, 0)",
            None,
        ),
    ],
)
def test_transform_age(
    sql: str, expected_sql: str, exec_checker, spark_session
) -> None:
    ast = sqlglot.parse_one(sql, dialect="postgres")
    ast = ast.transform(transform_age)
    transformed_sql = ast.sql(dialect="spark")

    assert transformed_sql == expected_sql

    if exec_checker is not None:
        exec_checker(sql, transformed_sql, spark_session)


def test_transform_age_lowercase_name():
    sql = "SELECT age('2025-12-02', '2024-11-01');"
    ast = sqlglot.parse_one(sql, dialect="postgres")
    ast = ast.transform(transform_age)
    transformed_sql = ast.sql(dialect="spark")

    assert "MAKE_INTERVAL" in transformed_sql
