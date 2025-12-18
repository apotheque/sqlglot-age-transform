from sqlglot import exp, Expression


def int_lit(n: int) -> exp.Literal:
    return exp.Literal.number(n)


def transform_age(node: Expression) -> Expression:
    if not isinstance(node, exp.Anonymous):
        return node

    name = str(node.this).upper()
    if name != "AGE":
        return node

    args = list(node.expressions)
    if len(args) == 2:
        end_date, start_date = args

    elif len(args) == 1:
        end_date = exp.CurrentTimestamp()
        start_date = args[0]

    else:
        raise ValueError(f"AGE expects 1 or 2 arguments, got {len(args)}")

    months_between_expr = exp.func("months_between", end_date, start_date)
    total_months = exp.Cast(
        this=months_between_expr,
        to=exp.DataType.build("INT"),
    )

    years = exp.Div(
        this=total_months.copy(),
        expression=int_lit(12),
    )

    months = exp.Mod(
        this=total_months.copy(),
        expression=int_lit(12),
    )

    days = exp.func(
        "datediff", end_date, exp.func("add_months", start_date, total_months.copy())
    )

    zero = int_lit(0)

    replacement = exp.Anonymous(
        this="make_interval",
        expressions=[years, months, zero, days, zero, zero, zero],
    )

    return replacement
