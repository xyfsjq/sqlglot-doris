from __future__ import annotations

import re

from sqlglot import exp, generator
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    count_if_to_sum,
    parse_timestamp_trunc,
    rename_func,
    time_format,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.dialects.tsql import DATE_DELTA_INTERVAL


def handle_date_trunc(self, expression: exp.DateTrunc | exp.DateTrunc_oracle) -> str:
    unit = self.sql(expression, "unit").strip("\"'").lower()
    this = self.sql(expression, "this")
    if unit.isalpha():
        mapped_unit = (
            DATE_DELTA_INTERVAL.get(unit) if DATE_DELTA_INTERVAL.get(unit) != None else unit
        )
        return f"DATE_TRUNC({this}, '{mapped_unit}')"
    elif unit.isdigit():
        return f"TRUNCATE({this}, {unit})"
    return f"DATE({this})"


def handle_date_diff(self, expression: exp.DateDiff) -> str:
    unit = self.sql(expression, "unit").lower()
    expressions = self.sql(expression, "expression")
    this = self.sql(expression, "this")
    if unit == "microsecond":
        return f"MICROSECONDS_DIFF({this},{expressions})"
    elif unit == "millisecond":
        return f"MILLISECONDS_DIFF({this},{expressions})"
    elif unit == "second":
        return f"SECONDS_DIFF({this},{expressions})"
    elif unit == "minute":
        return f"MINUTES_DIFF({this},{expressions})"
    elif unit == "hour":
        return f"HOURS_DIFF({this},{expressions})"
    elif unit == "day":
        return f"DATEDIFF({this},{expressions})"
    elif unit == "month":
        return f"MONTHS_DIFF({this},{expressions})"
    elif unit == "year":
        return f"YEARS_DIFF({this},{expressions})"
    return f"DATEDIFF({this},{expressions})"


#
# def handle_to_char(self, expression: exp.ToChar) -> str:
#     if self.sql(expression, "format") == "":
#         return f"DATE_FORMAT({self.sql(expression, 'this')}, '%H:%i:%s')"
#     decimal_places, has_decimal = parse_format_string(self.sql(expression, "format"))
#     if has_decimal:
#         return f"Round({self.sql(expression, 'this')},{decimal_places})"
#     else:
#         return f"DATE_FORMAT({self.sql(expression, 'this')}, {self.format_time(expression)})"


def parse_format_string(format_string):
    decimal_places = None
    number_pattern = r"^[-+]?\d+(\.\d+)?$"
    if re.match(number_pattern, format_string):
        decimal_places = len(format_string) - format_string.index(".") - 2
    has_decimal = decimal_places is not None
    return decimal_places, has_decimal


def handle_todecimal(self, expression: exp.ToDecimal) -> str:
    func_name = expression.meta["name"].lower()
    args = expression.args
    if func_name == "todecimal64":
        z = self.sql(args["this"])
        precision = self.sql(args["expressions"])
        return f"CAST({z} AS DECIMAL(38, {precision}))"
    return self.sql(expression)


def _str_to_unix_sql(self: generator.Generator, expression: exp.StrToUnix) -> str:
    return self.func("UNIX_TIMESTAMP", expression.this, time_format("doris")(self, expression))


def handle_array_concat(self, expression: exp.ArrayStringConcat) -> str:
    this = self.sql(expression, "this")
    expr = self.sql(expression, "expressions")
    if expr == "":
        return f"concat_ws('',{this})"
    return f"concat_ws({expr}, {this})"


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


def handle_replace(self, expression: exp.Replace) -> str:
    this = self.sql(expression, "this")
    old = self.sql(expression, "old")
    new = self.sql(expression, "new")
    if new == "":
        return f"REPLACE({this},{old},'')"
    return f"REPLACE({this},{old},{new})"


def extract_value_from_string(expression):
    # 匹配字符串中的数值或字符串值
    match = re.search(r"(\b\d+\b|\b\w+(?:\.\w+)*\b)", expression)

    if match:
        return match.group(0)
    else:
        return None


def arrow_json_extract_sql(self, expression: exp.JSONExtract) -> str:
    expr = self.sql(expression, "expression").strip("\"'")
    expr = extract_value_from_string(expr)
    if expr.isdigit():
        return f"jsonb_extract({self.sql(expression, 'this')},'$[{expr}]')"
    return f"jsonb_extract({self.sql(expression, 'this')},'$.{expr}')"


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
    return f"jsonb_extract({self.sql(expression, 'this')},'$.{path}')"


def arrow_json_extract_scalar_sql(self, expression: exp.JSONExtractScalar) -> str:
    # 将其中的数值或者字符串，提取出来
    expr = self.sql(expression, "expression").strip("\"'")
    expr = extract_value_from_string(expr)
    if expr.isdigit():
        return f"json_extract({self.sql(expression, 'this')},'$.[{expr}]')"
    return f"json_extract({self.sql(expression, 'this')},'$.{expr}')"


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
    return f"json_extract({self.sql(expression, 'this')},'$.{path}')"


def no_paren_current_date_sql(self, expression: exp.CurrentDate) -> str:
    zone = self.sql(expression, "this")
    return f"CURRENT_DATE() AT TIME ZONE {zone}" if zone else "CURRENT_DATE()"


def handle_regexp_extract(self, expr: exp.RegexpExtract) -> str:
    this = self.sql(expr, "this")
    expression = self.sql(expr, "expression")
    position = self.sql(expr, "position")
    if position == "":
        return f"REGEXP_EXTRACT_ALL({this}, '({expression[1:-1]})')"
    return f"REGEXP_EXTRACT({this}, '({expression[1:-1]})', {position})"


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
        separator = "\\n"
        return f"GROUP_CONCAT({self.format_args(this)}{order},'{separator}')"
    return f"GROUP_CONCAT({self.format_args(this, separator)}{order})"


def handle_concat_ws(self, expression: exp.ConcatWs) -> str:
    expression = expression.copy()
    delim, *rest_args = expression.expressions
    return f"CONCAT_WS({rest_args[0]},{delim})"


def handle_rand(self, expr: exp.Random) -> str:
    min = self.sql(expr, "this")
    max = self.sql(expr, "expression")
    if min == "" and max == "":
        return f"RANDOM()"
    elif max == "":
        return f"FLOOR(RANDOM()*{min}.0)"
    else:
        temp = int(max) - int(min)
        return f"FLOOR(RANDOM()*{temp}.0+{min}.0)"


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
    # 后面考虑改成doris的默认格式，暂时doris的2.0.0由于str_to_date对yyyy-MM-dd这些格式有点问题，已修复https://github.com/apache/doris/pull/22981
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"
    NULL_ORDERING = "nulls_are_frist"
    TIME_MAPPING = {
        **MySQL.TIME_MAPPING,
        "%Y": "yyyy",
        "%m": "MM",
        "%d": "dd",
        "%H": "HH",
        "%i": "mm",
        "%s": "ss",
        # 针对hive的日期格式的冲突
        # "%Y": '%%Y',
        # "%m": '%%-M',
        # "%d": '%%-d',
    }

    class Tokenizer(MySQL.Tokenizer):
        KEYWORDS = {
            **MySQL.Tokenizer.KEYWORDS,
            # "BITXOR": TokenType.CARET,
        }

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
            "FROM_UNIXTIME": exp.StrToUnix.from_arg_list,
            "GROUP_ARRAY": exp.ArrayAgg.from_arg_list,
            "LAST_DAY": exp.LastDateOfMonth.from_arg_list,
            "NOW": exp.CurrentTimestamp.from_arg_list,
            "REGEXP": exp.RegexpLike.from_arg_list,
            "SIZE": exp.ArraySize.from_arg_list,
            "SPLIT_BY_STRING": exp.RegexpSplit.from_arg_list,
            "SAMP": exp.StddevSamp.from_arg_list,
            "DATE_ADD": exp.DateAdd.from_arg_list,
            "DATE_SUB": exp.DateSub.from_arg_list,
            "DATE_DIFF": exp.DateDiff.from_arg_list,
            "TO_UNIXTIME": exp.UnixToStr.from_arg_list,
        }

    class Generator(MySQL.Generator):
        CAST_MAPPING = {}
        INTERVAL_ALLOWS_PLURAL_FORM = False
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
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.ArrayStringConcat: handle_array_concat,
            exp.ArrayToString: lambda self, e: f"ARRAY_JOIN({self.sql(e, 'this')},{self.sql(e, 'sep')}"
            + (f",{self.sql(e, 'null_replace')}" if self.sql(e, "null_replace") else "")
            + ")",
            exp.ArrayFilter: lambda self, e: f"ARRAY_FILTER({self.sql(e, 'expression')},{self.sql(e, 'this')})",
            exp.ArrayUniq: lambda self, e: f"SIZE(ARRAY_DISTINCT({self.sql(e, 'this')}))",
            exp.ArrayOverlaps: rename_func("ARRAYS_OVERLAP"),
            exp.ArrayUniqueAgg: rename_func("COLLECT_SET"),
            exp.BitwiseNot: rename_func("BITNOT"),
            exp.BitwiseAnd: rename_func("BITAND"),
            exp.BitwiseOr: rename_func("BITOR"),
            exp.BitwiseXor: rename_func("BITXOR"),
            exp.CastToStrType: lambda self, e: f"CAST({self.sql(e, 'this')} AS {self.sql(e, 'to')})",
            exp.CurrentTimestamp: lambda *_: "NOW()",
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CountIf: count_if_to_sum,
            exp.ConcatWs: handle_concat_ws,
            exp.DateDiff: handle_date_diff,
            exp.DateTrunc: handle_date_trunc,
            exp.DateTrunc_oracle: handle_date_trunc,
            # exp.Explode: rename_func("LATERAL VIEW EXPLODE"),
            exp.Filter: handle_filter,
            exp.GroupBitmap: lambda self, e: f"BITMAP_COUNT(BITMAP_AGG({self.sql(e, 'this')}))",
            exp.GroupBitmapAnd: lambda self, e: f"BITMAP_COUNT(BITMAP_INTERSECT({self.sql(e, 'this')}))",
            exp.GroupBitmapOr: lambda self, e: f"BITMAP_COUNT(BITMAP_UNION({self.sql(e, 'this')}))",
            exp.GroupConcat: _string_agg_sql,
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.JSONBExtract: arrow_jsonb_extract_sql,
            exp.JSONBExtractScalar: arrow_jsonb_extract_scalar_sql,
            exp.JSON_EXISTS_PATH: rename_func("JSON_EXISTS_PATH"),
            exp.JSONArrayContains: lambda self, e: f"JSON_CONTAINS({self.sql(e, 'this')},'{self.sql(e, 'expression')}')",
            exp.ParseJSON: rename_func("JSON_PARSE"),
            exp.JsonArrayLength: rename_func("JSON_LENGTH"),
            exp.LastDateOfMonth: rename_func("LAST_DAY"),
            exp.LTrim: rename_func("LTRIM"),
            exp.MICROSECONDS_ADD: rename_func("MICROSECONDS_ADD"),
            exp.MULTI_DISTINCT_COUNT: lambda self, e: f"COUNT(DISTINCT({self.sql(e, 'this')}))",
            exp.MULTI_DISTINCT_SUM: lambda self, e: f"SUM(DISTINCT({self.sql(e, 'this')}))",
            exp.RTrim: rename_func("RTRIM"),
            exp.Range: rename_func("ARRAY_RANGE"),
            exp.Random: handle_rand,
            exp.RegexpExtract: handle_regexp_extract,
            exp.RegexpLike: rename_func("REGEXP"),
            exp.RegexpSplit: rename_func("SPLIT_BY_STRING"),
            exp.Replace: handle_replace,
            exp.Repeat: rename_func("COUNTEQUAL"),
            exp.PERCENTILE_APPROX: rename_func("PERCENTILE_APPROX"),
            exp.RETENTION: rename_func("RETENTION"),
            exp.SHA256: lambda self, e: f"SHA2({self.sql(e, 'this')},256)",
            exp.SortArray: rename_func("ARRAY_SORT"),
            exp.StrPosition: lambda self, e: (
                f"LOCATE({self.sql(e, 'substr')}, {self.sql(e, 'this')}, {self.sql(e, 'instance')})"
                if self.sql(e, "instance")
                else f"LOCATE({self.sql(e, 'substr')}, {self.sql(e, 'this')})"
            ),
            exp.StrToUnix: _str_to_unix_sql,
            exp.Split: rename_func("SPLIT_BY_STRING"),
            exp.DPipe: lambda self, e: f"CONCAT({self.sql(e,'this')},{self.sql(e,'expression')})",
            exp.Shuffle: rename_func("ARRAY_SHUFFLE"),
            exp.Slice: rename_func("ARRAY_SLICE"),
            exp.StAstext: handle_geography,
            exp.TimeStrToDate: rename_func("TO_DATE"),
            # exp.ToChar: handle_to_char,
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, INTERVAL {self.sql(e, 'expression')} {self.sql(e, 'unit')})",
            exp.TsOrDsToDate: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATE)",
            exp.TimeStrToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimeToUnix: rename_func("UNIX_TIMESTAMP"),
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", e.this, "'" + e.text("unit") + "'"
            ),
            exp.ToDecimal: handle_todecimal,  # ck的强转TODECIMAL64
            exp.ToDatetime: lambda self, e: f"CAST({self.sql(e, 'this')} AS DATETIME)",
            exp.Today: lambda self, e: f"TO_DATE(NOW())",
            exp.ToYyyymm: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, '%Y%m')",
            exp.ToYyyymmdd: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, '%Y%m%d')",
            exp.ToYyyymmddhhmmss: lambda self, e: f"DATE_FORMAT({self.sql(e, 'this')}, '%Y%m%d%H%i%s')",
            exp.ToStartOfQuarter: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Quarter')",
            exp.ToStartOfMonth: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Month')",
            exp.ToStartOfWeek: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Week')",
            exp.ToStartOfDay: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Day')",
            exp.ToStartOfHour: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Hour')",
            exp.ToStartOfMinute: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Minute')",
            exp.ToStartOfSecond: lambda self, e: f"DATE_TRUNC({self.sql(e, 'this')}, 'Second')",
            exp.UnixToStr: lambda self, e: self.func(
                "FROM_UNIXTIME", e.this, time_format("doris")(self, e)
            ),
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
            exp.QuartersAdd: lambda self, e: f"MONTHS_ADD({self.sql(e, 'this')},{3 * int(self.sql(e, 'expression'))})",
            exp.QuartersSub: lambda self, e: f"MONTHS_SUB({self.sql(e, 'this')},{3 * int(self.sql(e, 'expression'))})",
        }

        def currentdate_sql(self, expression: exp.CurrentDate) -> str:
            zone = self.sql(expression, "this")
            return f"CURRENT_DATE({zone})" if zone else "CURRENT_DATE()"

        def parameter_sql(self, expression: exp.Parameter) -> str:
            this = self.sql(expression, "this")
            expression_sql = self.sql(expression, "expression")

            parent = expression.parent
            this = f"{this}:{expression_sql}" if expression_sql else this

            if isinstance(parent, exp.EQ) and isinstance(parent.parent, exp.SetItem):
                # We need to produce SET key = value instead of SET ${key} = value
                return this

            return f"${{{this}}}"
