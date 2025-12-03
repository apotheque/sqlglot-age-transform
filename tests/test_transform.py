import sqlglot
from sqlglot_age_transform.transform import transform_age
import pytest

import duckdb
from pyspark.sql import SparkSession


def test_execution_results():
    sql = "SELECT AGE('2025-12-02'::DATE, '2024-11-01'::DATE) AS age"

    duck_interval = duckdb.sql(
        "SELECT "
        "EXTRACT(YEAR FROM age) AS y, "
        "EXTRACT(MONTH FROM age) AS m, "
        "EXTRACT(DAY FROM age) AS d "
        f"FROM ({sql}) t"
    ).fetchone()

    assert duck_interval == (1, 1, 1)

    ast = sqlglot.parse_one(sql, dialect="postgres")
    for node in ast.walk():
        transform_age(node)
    transformed_sql = ast.sql(dialect="spark")

    spark = SparkSession.builder.master("local[*]").appName("age_test").getOrCreate()

    spark_interval_query = (
        "SELECT "
        "EXTRACT(YEAR FROM age) AS y, "
        "EXTRACT(MONTH FROM age) AS m, "
        "EXTRACT(DAY FROM age) AS d "
        f"FROM ({transformed_sql}) t"
    )

    df = spark.sql(spark_interval_query)
    spark_interval = tuple(df.collect()[0])
    spark.stop()

    assert spark_interval == duck_interval


@pytest.mark.parametrize(
    "sql, expected_sql",
    [
        (
            "SELECT AGE('2025-12-02', '2024-11-01');",
            "SELECT MAKE_INTERVAL("
            "CAST(MONTHS_BETWEEN(CAST('2025-12-02' AS DATE), CAST('2024-11-01' AS DATE)) AS INT) / 12, "
            "CAST(MONTHS_BETWEEN(CAST('2025-12-02' AS DATE), CAST('2024-11-01' AS DATE)) AS INT) % 12, "
            "0, "
            "DATEDIFF(CAST('2025-12-02' AS DATE), "
            "ADD_MONTHS(CAST('2024-11-01' AS DATE), "
            "CAST(MONTHS_BETWEEN(CAST('2025-12-02' AS DATE), CAST('2024-11-01' AS DATE)) AS INT))), "
            "0, 0, 0)",
        ),
        (
            "SELECT AGE('2025-12-02');",
            "SELECT MAKE_INTERVAL("
            "CAST(MONTHS_BETWEEN(CAST(CURRENT_TIMESTAMP() AS DATE), CAST('2025-12-02' AS DATE)) AS INT) / 12, "
            "CAST(MONTHS_BETWEEN(CAST(CURRENT_TIMESTAMP() AS DATE), CAST('2025-12-02' AS DATE)) AS INT) % 12, "
            "0, "
            "DATEDIFF(CAST(CURRENT_TIMESTAMP() AS DATE), "
            "ADD_MONTHS(CAST('2025-12-02' AS DATE), "
            "CAST(MONTHS_BETWEEN(CAST(CURRENT_TIMESTAMP() AS DATE), CAST('2025-12-02' AS DATE)) AS INT))), "
            "0, 0, 0)",
        ),
        (
            "SELECT CASE WHEN last_expiration_dt IS NOT NULL "
            "THEN EXTRACT(YEAR FROM AGE('2024-12-31'::DATE, last_expiration_dt)) "
            "ELSE NULL END AS airb_eff_maturity_year "
            "FROM custom_risk_cred_port_lp.t_u_agr_cred;",
            "MAKE_INTERVAL("
            "CAST(MONTHS_BETWEEN(CAST('2024-12-31' AS DATE), CAST(last_expiration_dt AS DATE)) AS INT) / 12, "
            "CAST(MONTHS_BETWEEN(CAST('2024-12-31' AS DATE), CAST(last_expiration_dt AS DATE)) AS INT) % 12, "
            "0, "
            "DATEDIFF(CAST('2024-12-31' AS DATE), "
            "ADD_MONTHS(CAST(last_expiration_dt AS DATE), "
            "CAST(MONTHS_BETWEEN(CAST('2024-12-31' AS DATE), CAST(last_expiration_dt AS DATE)) AS INT))), "
            "0, 0, 0)",
        ),
    ],
)
def test_transform_age(sql: str, expected_sql: str) -> None:
    ast = sqlglot.parse_one(sql, dialect="postgres")
    for node in ast.walk():
        transform_age(node)
    transformed_sql = ast.sql(dialect="spark")
    assert expected_sql in transformed_sql
