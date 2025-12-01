import sqlglot

from sqlglot_age_transform.transform import transform_age


# TODO: add asserts and more test cases
def test_transform_age():
    sql = "SELECT AGE('2025-12-02', '2024-11-01');"
    ast = sqlglot.parse_one(sql, dialect="postgres")
    for node in ast.walk():
        transform_age(node)
    transformed_sql = ast.sql(dialect="spark")
    print(transformed_sql)
