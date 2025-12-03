from sqlglot import exp, Expression


def cast_to_date(expr: Expression) -> Expression:
    if isinstance(expr, exp.Cast):
        to_type = expr.args.get("to")
        if isinstance(to_type, exp.DataType) and to_type.this == exp.DataType.Type.DATE:
            return expr
    return exp.Cast(this=expr, to=exp.DataType.build("DATE"))


def int_lit(n: int) -> exp.Literal:
    return exp.Literal.number(n)


def transform_age(node: Expression) -> Expression:
    # TODO: rewrite AGE(x, y) | AGE(x) → equivalent Spark SQL expression

    if not isinstance(node, exp.Anonymous):
        return node

    name = node.this
    if name != "AGE":
        return node

    args = list(node.expressions)
    if len(args) == 2:
        end_date = cast_to_date(args[0])
        start_date = cast_to_date(args[1])

    elif len(args) == 1:
        end_date = cast_to_date(exp.CurrentTimestamp())
        start_date = cast_to_date(args[0])

    else:
        return node

    months_between_expr = exp.Anonymous(
        this="months_between",
        expressions=[end_date, start_date],
    )
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

    days = exp.Anonymous(
        this="datediff",
        expressions=[
            end_date,
            exp.Anonymous(
                this="add_months",
                expressions=[
                    start_date,
                    total_months.copy(),
                ],
            ),
        ],
    )

    zero = int_lit(0)

    replacement = exp.Anonymous(
        this="make_interval",
        expressions=[years, months, zero, days, zero, zero, zero],
    )
    node.replace(replacement)
    return replacement
