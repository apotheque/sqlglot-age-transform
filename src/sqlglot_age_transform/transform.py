from sqlglot import Expression


def transform_age(node: Expression) -> Expression:
    # TODO: rewrite AGE(x, y) | AGE(x) → equivalent Spark SQL expression
    raise NotImplementedError
