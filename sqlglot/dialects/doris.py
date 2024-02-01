from __future__ import annotations

import re
import typing as t

from sqlglot import exp, generator
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    count_if_to_sum,
    parse_timestamp_trunc,
    prepend_dollar_to_path,
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


def handle_concat_ws(self, expression: exp.ConcatWs) -> str:
    delim, *rest_args = expression.expressions
    rest_args_sql = ", ".join(self.sql(arg) for arg in rest_args)
    return f"CONCAT_WS({self.sql(delim)}, {rest_args_sql})"


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
    if unit.isdigit() or unit.lstrip("-").isdigit() or this.isdigit():
        if this.isdigit():
            return f"TRUNCATE({this})"
        return f"TRUNCATE({this}, {unit})"
    return f"DATE({this})"


def handle_geography(
    self, expression: exp.StAstext
) -> str:  # Realize the identification of geography
    this = self.sql(expression, "this").upper()
    match = re.search(r"POINT\(([-\d.]+) ([-\d.]+)\)", this)
    if match is None:
        return f"ST_ASTEXT(ST_GEOMETRYFROMWKB({this}))"
    x = float(match.group(1))
    y = float(match.group(2))
    return f"ST_ASTEXT(ST_POINT{x, y})"


def handle_log(self, expression: exp.Log) -> str:
    this = self.sql(expression, "this")
    expression = self.sql(expression, "expression")

    if expression == "":
        return self.func("LOG10", this)
    return self.func("LOG", this, expression)


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


def _string_agg_sql(self: Doris.Generator, expression: exp.GroupConcat) -> str:
    expression = expression.copy()
    separator = expression.args.get("separator") or exp.Literal.string(",")

    order = ""
    this = expression.this
    if isinstance(this, exp.Order):
        if this.this:
            this = this.this.pop()
        order = self.sql(expression.this)  # Order has a leading space
    if isinstance(separator, exp.Chr):
        separator = "\n"
        return f"GROUP_CONCAT({self.format_args(this)}{order},'{separator}')"
    return f"GROUP_CONCAT({self.format_args(this, separator)}{order})"


def handle_regexp_extract(self, expr: exp.RegexpExtract) -> str:
    this = self.sql(expr, "this")
    expression = self.sql(expr, "expression")
    position = self.sql(expr, "position")
    if position == "":
        return f"REGEXP_EXTRACT_ALL({this}, '({expression[1:-1]})')"
    return f"REGEXP_EXTRACT({this}, '({expression[1:-1]})', {position})"


def handle_to_date(self: Doris.Generator, expression: exp.TsOrDsToDate) -> str:
    this = self.sql(expression, "this")
    time_format = self.format_time(expression)
    if time_format and time_format not in (Doris.TIME_FORMAT, Doris.DATE_FORMAT):
        return f"DATE_FORMAT({this}, {time_format})"
    if isinstance(expression.this, exp.TsOrDsToDate):
        return this
    return f"TO_DATE({this})"


def handle_replace(self, expression: exp.Replace) -> str:
    this = self.sql(expression, "this")
    old = self.sql(expression, "old")
    new = self.sql(expression, "new")
    if new == "":
        return f"REPLACE({this},{old},'')"
    return f"REPLACE({this},{old},{new})"


def handle_rand(self, expr: exp.Rand) -> str:
    min = self.sql(expr, "this")
    max = self.sql(expr, "expression")
    if min == "" and max == "":
        return f"RANDOM()"
    elif max == "":
        return f"FLOOR(RANDOM()*{min}.0)"
    else:
        temp = int(max) - int(min)
        return f"FLOOR(RANDOM()*{temp}.0+{min}.0)"


def _str_to_unix_sql(self: generator.Generator, expression: exp.StrToUnix) -> str:
    return self.func("UNIX_TIMESTAMP", expression.this, time_format("doris")(self, expression))


# Matches a numeric or string value in a string
def extract_value_from_string(expression):
    match = re.search(r"(\b\d+\b|\b\w+(?:\.\w+)*\b)", expression)
    if match:
        return match.group(0)
    else:
        return None


def arrow_json_extract_sql(self, expression: exp.JSONExtract) -> str:
    expr = self.sql(expression, "expression").strip("\"'")
    expr = extract_value_from_string(expr)
    if expr.isdigit():
        return f"JSONB_EXTRACT({self.sql(expression, 'this')},'$[{expr}]')"
    return f"JSONB_EXTRACT({self.sql(expression, 'this')},'$.{expr}')"


def arrow_jsonb_extract_sql(self, expression: exp.JSONBExtract) -> str:
    expr = self.sql(expression, "expression").strip("\"'")
    values = [key.strip() for key in expr[1:-1].split(",")]
    json_path = []
    for value in values:
        if value.isdigit():
            json_path.append(f"[{value}]")
        else:
            json_path.append(value)
    path = ".".join(json_path)
    return f"JSONB_EXTRACT({self.sql(expression, 'this')},'$.{path}')"


def arrow_json_extract_scalar_sql(self, expression: exp.JSONExtractScalar) -> str:
    # Extract the numerical values or strings among them
    expr = self.sql(expression, "expression").strip("\"'")
    expr = extract_value_from_string(expr)
    if expr.isdigit():
        return f"JSON_EXTRACT({self.sql(expression, 'this')},'$.[{expr}]')"
    return f"JSON_EXTRACT({self.sql(expression, 'this')},'$.{expr}')"


def arrow_jsonb_extract_scalar_sql(self, expression: exp.JSONBExtractScalar) -> str:
    expr = self.sql(expression, "expression").strip("\"'")
    values = [key.strip() for key in expr[1:-1].split(",")]
    json_path = []
    for value in values:
        if value.isdigit():
            json_path.append(f"[{value}]")
        else:
            json_path.append(value)
    path = ".".join(json_path)
    return f"JSON_EXTRACT({self.sql(expression, 'this')},'$.{path}')"


def _json_extract_sql(
    self: Doris.Generator,
    expression: exp.JSONExtract | exp.JSONExtractScalar | exp.JSONBExtract | exp.JSONBExtractScalar,
) -> str:
    return self.func(
        "JSONB_EXTRACT",
        expression.this,
        expression.expression,
        # *json_path_segments(self, expression.expression),
        expression.args.get("null_if_invalid"),
    )


def path_to_jsonpath(
    name: str = "JSONB_EXTRACT",
) -> t.Callable[[generator.Generator, exp.GetPath], str]:
    def _transform(self: generator.Generator, expression: exp.GetPath) -> str:
        return rename_func(name)(self, prepend_dollar_to_path(expression))

    return _transform


class Doris(MySQL):
    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"
    NULL_ORDERING = "nulls_are_frist"
    DPIPE_IS_STRING_CONCAT = True
    TIME_MAPPING = {
        **MySQL.TIME_MAPPING,
        "%Y": "yyyy",
        "%m": "MM",
        "%d": "dd",
        "%s": "ss",
        "%H": "HH24",
        "%H": "hh24",  # oracle to_date('2005-01-01 13:14:20','yyyy-MM-dd hh24:mm:ss')
        "%H": "HH",
        "%i": "mm",
    }

    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "ARRAY_SHUFFLE": exp.Shuffle.from_arg_list,
            "ARRAY_RANGE": exp.Range.from_arg_list,
            "ARRAY_SORT": exp.SortArray.from_arg_list,
            "COLLECT_LIST": exp.ArrayAgg.from_arg_list,
            "COLLECT_SET": exp.ArrayUniqueAgg.from_arg_list,
            "TRUNCATE": exp.Truncate.from_arg_list,
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
            "TO_DATE": exp.TsOrDsToDate.from_arg_list,
        }

        def _parse_explain(self) -> exp.Explain:
            this = "explain"
            comments = self._prev_comments
            return self.expression(
                exp.Explain,
                comments=comments,
                **{  # type: ignore
                    "this": this,
                    "expressions": self._parse_select(nested=True),
                },
            )

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
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.ApproxQuantile: rename_func("PERCENTILE_APPROX"),
            exp.ArgMax: rename_func("MAX_BY"),
            exp.ArgMin: rename_func("MIN_BY"),
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.ArrayFilter: lambda self, e: f"ARRAY_FILTER({self.sql(e, 'expression')},{self.sql(e, 'this')})",
            exp.ArrayUniq: lambda self, e: f"SIZE(ARRAY_DISTINCT({self.sql(e, 'this')}))",
            exp.ArrayOverlaps: rename_func("ARRAYS_OVERLAP"),
            exp.ArrayPosition: rename_func("ELEMENT_AT"),
            exp.ArrayStringConcat: handle_array_concat,
            exp.ArrayToString: handle_array_to_string,
            exp.ArrayUniqueAgg: rename_func("COLLECT_SET"),
            exp.BitwiseNot: rename_func("BITNOT"),
            exp.BitwiseAnd: rename_func("BITAND"),
            exp.BitwiseOr: rename_func("BITOR"),
            exp.BitwiseXor: rename_func("BITXOR"),
            exp.BitmapXOrCount: rename_func("BITMAP_XOR_COUNT"),
            exp.CastToStrType: lambda self, e: f"CAST({self.sql(e, 'this')} AS {self.sql(e, 'to')})",
            exp.ConcatWs: handle_concat_ws,
            exp.CountIf: count_if_to_sum,
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CurrentTimestamp: lambda *_: "NOW()",
            exp.DateDiff: handle_date_diff,
            exp.DPipe: lambda self, e: f"CONCAT({self.sql(e, 'this')},{self.sql(e, 'expression')})",
            exp.DateTrunc: handle_date_trunc,
            exp.Empty: rename_func("NULL_OR_EMPTY"),
            exp.Filter: handle_filter,
            exp.GroupConcat: _string_agg_sql,
            exp.GetPath: path_to_jsonpath(),
            exp.JSONExtractScalar: _json_extract_sql,
            exp.JSONExtract: _json_extract_sql,
            exp.JSONBExtract: _json_extract_sql,
            exp.JSONBExtractScalar: _json_extract_sql,
            exp.JSONArrayContains: rename_func("JSON_CONTAINS"),
            exp.ParseJSON: rename_func("JSON_PARSE"),
            exp.JsonArrayLength: rename_func("JSON_LENGTH"),
            exp.Log: handle_log,
            exp.LTrim: rename_func("LTRIM"),
            exp.Map: rename_func("ARRAY_MAP"),
            exp.NotEmpty: rename_func("NOT_NULL_OR_EMPTY"),
            exp.QuartersAdd: lambda self, e: f"MONTHS_ADD({self.sql(e, 'this')},{3 * int(self.sql(e, 'expression'))})",
            exp.QuartersSub: lambda self, e: f"MONTHS_SUB({self.sql(e, 'this')},{3 * int(self.sql(e, 'expression'))})",
            exp.Rand: handle_rand,
            exp.RegexpLike: rename_func("REGEXP"),
            exp.RegexpExtract: handle_regexp_extract,
            exp.RegexpSplit: rename_func("SPLIT_BY_STRING"),
            exp.Replace: handle_replace,
            exp.RTrim: rename_func("RTRIM"),
            exp.SHA2: lambda self, e: f"SHA2({self.sql(e, 'this')},{self.sql(e, 'length')})",
            exp.Shuffle: rename_func("ARRAY_SHUFFLE"),
            exp.Slice: rename_func("ARRAY_SLICE"),
            exp.SortArray: rename_func("ARRAY_SORT"),
            exp.Split: rename_func("SPLIT_BY_STRING"),
            exp.StAstext: handle_geography,
            exp.StrPosition: lambda self, e: (
                f"LOCATE({self.sql(e, 'substr')}, {self.sql(e, 'this')}, {self.sql(e, 'instance')})"
                if self.sql(e, "instance")
                else f"LOCATE({self.sql(e, 'substr')}, {self.sql(e, 'this')})"
            ),
            exp.StrToUnix: _str_to_unix_sql,
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", e.this, "'" + e.text("unit") + "'"
            ),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.ToChar: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.Today: lambda self, e: f"TO_DATE(NOW())",
            exp.ToStartOfDay: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Day')",
            exp.ToStartOfHour: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Hour')",
            exp.ToStartOfMinute: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Minute')",
            exp.ToStartOfMonth: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Month')",
            exp.ToStartOfQuarter: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Quarter')",
            exp.ToStartOfSecond: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Second')",
            exp.ToStartOfWeek: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Week')",
            exp.ToYyyymm: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, '%Y%m')",
            exp.ToYyyymmdd: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, '%Y%m%d')",
            exp.ToYyyymmddhhmmss: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, '%Y%m%d%H%i%s')",
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, {self.sql(e, 'expression')})",  # Only for day level
            exp.TsOrDsToDate: handle_to_date,
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME", e.this, time_format("doris")(self, e)
            ),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.Variance: rename_func("VAR_SAMP"),
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

        def explain_sql(self, expression: exp.Explain) -> str:
            this = self.sql(expression, "this")
            expr = self.sql(expression, "expressions")
            return f"{this} {expr}"
