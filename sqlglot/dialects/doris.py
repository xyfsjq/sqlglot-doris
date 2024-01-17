from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    count_if_to_sum,
    parse_timestamp_trunc,
    rename_func,
    time_format,
)
from sqlglot.dialects.mysql import MySQL

DATE_DELTA_INTERVAL = {
    "year": "year",
    "yyyy": "year",
    "yy": "year",
    "quarter": "quarter",
    "qq": "quarter",
    "q": "quarter",
    "month": "month",
    "mm": "month",
    "m": "month",
    "week": "week",
    "ww": "week",
    "wk": "week",
    "day": "day",
    "dd": "day",
    "d": "day",
}


def no_paren_current_date_sql(self, expression: exp.CurrentDate) -> str:
    zone = self.sql(expression, "this")
    return f"CURRENT_DATE() AT TIME ZONE {zone}" if zone else "CURRENT_DATE()"


def handle_array_concat(self, expression: exp.ArrayStringConcat) -> str:
    this = self.sql(expression, "this")
    expr = self.sql(expression, "expressions")
    if expr == "":
        return f"CONCAT_WS('',{this})"
    return f"CONCAT_WS({expr}, {this})"


def handle_array_to_string(self, expression: exp.ArrayToString) -> str:
    this = self.sql(expression, "this")
    sep = self.sql(expression, "sep")
    null_replace = self.sql(expression, "null_replace")
    result = f"ARRAY_JOIN({this},{sep}"
    if null_replace:
        result += f",{null_replace}"

    result += ")"

    return result


def handle_date_diff(self, expression: exp.DateDiff) -> str:
    unit = self.sql(expression, "unit").lower()
    expressions = self.sql(expression, "expression")
    this = self.sql(expression, "this")
    if unit == "microsecond":
        return f"MICROSECONDS_DIFF({this}, {expressions})"
    elif unit == "millisecond":
        return f"MILLISECONDS_DIFF({this}, {expressions})"
    elif unit == "second":
        return f"SECONDS_DIFF({this}, {expressions})"
    elif unit == "minute":
        return f"MINUTES_DIFF({this}, {expressions})"
    elif unit == "hour":
        return f"HOURS_DIFF({this}, {expressions})"
    elif unit == "day":
        return f"DATEDIFF({this}, {expressions})"
    elif unit == "month":
        return f"MONTHS_DIFF({this}, {expressions})"
    elif unit == "year":
        return f"YEARS_DIFF({this}, {expressions})"
    return f"DATEDIFF({this}, {expressions})"


def handle_date_trunc(self, expression: exp.DateTrunc) -> str:
    unit = self.sql(expression, "unit").strip("\"'").lower()
    this = self.sql(expression, "this")
    if unit.isalpha():
        mapped_unit = DATE_DELTA_INTERVAL.get(unit) or unit
        return f"DATE_TRUNC({this}, '{mapped_unit}')"
    if unit.isdigit():
        return f"TRUNCATE({this}, {unit})"
    return f"DATE({this})"


def handle_filter(self, expr: exp.Filter) -> str:
    expression = expr.copy()
    self.sql(expr, "this")
    expr = expression.expression.args["this"]
    agg = expression.this.key
    spec = expression.this.args["this"]
    case = (
        exp.Case()
        .when(
            expr,
            spec,
        )
        .else_("0")
    )
    return f"{agg}({self.sql(case)})"


class Doris(MySQL):
    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"
    NULL_ORDERING = "nulls_are_frist"

    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "ARRAY_SHUFFLE": exp.Shuffle.from_arg_list,
            "ARRAY_RANGE": exp.Range.from_arg_list,
            "ARRAY_SORT": exp.SortArray.from_arg_list,
            "COUNTEQUAL": exp.Repeat.from_arg_list,
            "COLLECT_LIST": exp.ArrayAgg.from_arg_list,
            "COLLECT_SET": exp.ArrayUniqueAgg.from_arg_list,
            "DATE_TRUNC": parse_timestamp_trunc,
            "DATE_ADD": exp.DateAdd.from_arg_list,
            "DATE_SUB": exp.DateSub.from_arg_list,
            "DATEDIFF": exp.DateDiff.from_arg_list,
            "FROM_UNIXTIME": exp.StrToUnix.from_arg_list,
            "GROUP_ARRAY": exp.ArrayAgg.from_arg_list,
            "NOW": exp.CurrentTimestamp.from_arg_list,
            "REGEXP": exp.RegexpLike.from_arg_list,
            "SIZE": exp.ArraySize.from_arg_list,
            "SPLIT_BY_STRING": exp.RegexpSplit.from_arg_list,
            "VAR_SAMP": exp.StddevSamp.from_arg_list,
            "TO_DATE": exp.TsOrDsToDate.from_arg_list,
        }

    class Generator(MySQL.Generator):
        CAST_MAPPING = {}
        INTERVAL_ALLOWS_PLURAL_FORM = False
        LAST_DAY_SUPPORTS_DATE_PART = False
        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME",
        }

        TIMESTAMP_FUNC_TYPES = set()

        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.ArgMax: rename_func("MAX_BY"),
            exp.ArgMin: rename_func("MIN_BY"),
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ApproxQuantile: rename_func("PERCENTILE_APPROX"),
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.ArrayFilter: lambda self, e: f"ARRAY_FILTER({self.sql(e, 'expression')},{self.sql(e, 'this')})",
            exp.ArrayOverlaps: rename_func("ARRAYS_OVERLAP"),
            exp.BitwiseNot: rename_func("BITNOT"),
            exp.BitwiseAnd: rename_func("BITAND"),
            exp.BitwiseOr: rename_func("BITOR"),
            exp.BitwiseXor: rename_func("BITXOR"),
            exp.ArrayStringConcat: handle_array_concat,
            exp.ArrayToString: handle_array_to_string,
            exp.ArrayUniq: lambda self, e: f"SIZE(ARRAY_DISTINCT({self.sql(e, 'this')}))",
            exp.ArrayUniqueAgg: rename_func("COLLECT_SET"),
            exp.CastToStrType: lambda self, e: f"CAST({self.sql(e, 'this')} AS {self.sql(e, 'to')})",
            exp.CurrentTimestamp: lambda *_: "NOW()",
            exp.DateDiff: handle_date_diff,
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CountIf: count_if_to_sum,
            exp.DateTrunc: handle_date_trunc,
            exp.Filter: handle_filter,
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.Map: rename_func("ARRAY_MAP"),
            exp.RegexpLike: rename_func("REGEXP"),
            exp.RegexpSplit: rename_func("SPLIT_BY_STRING"),
            exp.StrToUnix: lambda self, e: f"UNIX_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.Split: rename_func("SPLIT_BY_STRING"),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.ToChar: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",  # Only for day level
            exp.TsOrDsToDate: lambda self, e: self.func("TO_DATE", e.this),
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", e.this, "'" + e.text("unit") + "'"
            ),
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME", e.this, time_format("doris")(self, e)
            ),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
        }

        def parameter_sql(self, expression: exp.Parameter) -> str:
            this = self.sql(expression, "this")
            expression_sql = self.sql(expression, "expression")

            parent = expression.parent
            this = f"{this}:{expression_sql}" if expression_sql else this

            if isinstance(parent, exp.EQ) and isinstance(parent.parent, exp.SetItem):
                # We need to produce SET key = value instead of SET ${key} = value
                return this

            return f"${{{this}}}"
