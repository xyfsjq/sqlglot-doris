from __future__ import annotations

import re

from sqlglot import exp, generator
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    parse_timestamp_trunc,
    rename_func,
    time_format,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.dialects.tsql import DATE_DELTA_INTERVAL


def handle_date_trunc(self, expression: exp.DateTrunc) -> str:
    this = self.sql(expression, "unit")
    unit = self.sql(expression, "this").strip("\"'").lower()
    if unit.isalpha():
        mapped_unit = (
            DATE_DELTA_INTERVAL.get(unit) if DATE_DELTA_INTERVAL.get(unit) != None else unit
        )
        return f"DATE_TRUNC({mapped_unit}, {this})"
    elif unit.isdigit():
        return f"TRUNCATE({this},{unit})"
    return f"DATE({unit})"

def handle_date_diff(self, expression: exp.DateDiff) -> str:
    this = self.sql(expression, "unit")
    expressions = self.sql(expression, "expression")
    unit = self.sql(expression, "this").lower()
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

def handle_to_char(self, expression: exp.ToChar) -> str:
    if self.sql(expression, "format") == "":
        return f"DATE_FORMAT({self.sql(expression, 'this')}, '%H:%i:%s')"
    decimal_places, has_decimal = parse_format_string(self.sql(expression, "format"))
    if has_decimal:
        return f"Round({self.sql(expression, 'this')},{decimal_places})"
    else:
        return f"DATE_FORMAT({self.sql(expression, 'this')}, {self.format_time(expression)})"


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


def arrow_json_extract_sql(self, expression: exp.JSONExtract) -> str:
    expr = self.sql(expression, "expression").strip("\"'")
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
    expr = self.sql(expression, "expression").strip("\"'")
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


class Doris(MySQL):
    DATE_FORMAT = "'yyyy-MM-dd'"
    DATEINT_FORMAT = "'yyyyMMdd'"
    # 后面考虑改成doris的默认格式，暂时doris的2.0.0由于str_to_date对yyyy-MM-dd这些格式有点问题，已修复https://github.com/apache/doris/pull/22981
    TIME_FORMAT = "'yyyy-MM-dd HH:mm:ss'"

    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "DATE_TRUNC": parse_timestamp_trunc,
            "REGEXP": exp.RegexpLike.from_arg_list,
            "FROM_UNIXTIME": exp.StrToUnix.from_arg_list,
            "SIZE": exp.ArraySize.from_arg_list,
            "COLLECT_LIST": exp.ArrayAgg.from_arg_list,
        }

    class Generator(MySQL.Generator):
        CAST_MAPPING = {}
        INTERVAL_ALLOWS_PLURAL_FORM = False
        # case_sensitive
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
            exp.ArrayAgg: rename_func("COLLECT_LIST"),
            exp.ArraySize: rename_func("SIZE"),
            exp.ArrayStringConcat: handle_array_concat,
            exp.ArrayFilter: lambda self, e: f"ARRAY_FILTER({self.sql(e, 'expression')},{self.sql(e, 'this')})",
            exp.ArrayUniq: lambda self, e: f"SIZE(ARRAY_DISTINCT({self.sql(e, 'this')}))",
            exp.ArrayOverlaps: rename_func("ARRAYS_OVERLAP"),
            exp.BitwiseNot: rename_func("BITNOT"),
            exp.BitwiseAnd: rename_func("BITAND"),
            exp.BitwiseOr: rename_func("BITOR"),
            exp.BitwiseXor: rename_func("BITXOR"),
            exp.CurrentTimestamp: lambda *_: "NOW()",
            exp.CurrentDate: no_paren_current_date_sql,
            exp.DateDiff: handle_date_diff,
            exp.DateTrunc: handle_date_trunc,
            exp.Explode: rename_func("LATERAL VIEW EXPLODE"),
            exp.GroupBitmap: lambda self, e: f"BITMAP_COUNT(BITMAP_AGG({self.sql(e, 'this')}))",
            exp.GroupBitmapAnd: lambda self, e: f"BITMAP_COUNT(BITMAP_INTERSECT({self.sql(e, 'this')}))",
            exp.GroupBitmapOr: lambda self, e: f"BITMAP_COUNT(BITMAP_UNION({self.sql(e, 'this')}))",
            exp.JSONExtractScalar: arrow_json_extract_scalar_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.JSONBExtract: arrow_jsonb_extract_sql,
            exp.JSONBExtractScalar: arrow_jsonb_extract_scalar_sql,
            exp.LTrim: rename_func("LTRIM"),
            exp.RTrim: rename_func("RTRIM"),
            exp.Range: rename_func("ARRAY_RANGE"),
            exp.RegexpExtract: handle_regexp_extract,
            exp.RegexpLike: rename_func("REGEXP"),
            exp.RegexpSplit: rename_func("SPLIT_BY_STRING"),
            exp.Replace: handle_replace,
            exp.Repeat: rename_func("COUNTEQUAL"),
            exp.SetAgg: rename_func("COLLECT_SET"),
            exp.SortArray: rename_func("ARRAY_SORT"),
            exp.StrPosition: lambda self, e: f"LOCATE({self.sql(e, 'substr')}, {self.sql(e, 'this')})",
            exp.StrToUnix: _str_to_unix_sql,
            exp.Split: rename_func("SPLIT_BY_STRING"),
            exp.SafeDPipe: rename_func("CONCAT"),
            exp.Shuffle: rename_func("ARRAY_SHUFFLE"),
            exp.Slice: rename_func("ARRAY_SLICE"),
            exp.StAstext: handle_geography,
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.ToChar: handle_to_char,
            exp.TsOrDsAdd: lambda self, e: f"DATE_ADD({self.sql(e, 'this')}, INTERVAL {self.sql(e, 'expression')} {self.sql(e, 'unit')})",
            # Only for day level
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

        # def sub_sql(self, expression: exp.Sub) -> str:
        #     if re.search(r'date', self.sql(expression, 'this'), re.IGNORECASE):
        #         this = self.sql(expression, 'this')
        #         expr = self.sql(expression, 'expression')
        #         return f"{this} - INTERVAL {expr} DAY"
        #     return self.binary(expression, "-")
        #
        # def add_sql(self, expression: exp.Add) -> str:
        #     if re.search(r'date', self.sql(expression, 'this'), re.IGNORECASE):
        #         this = self.sql(expression, 'this')
        #         expr = self.sql(expression,'expression')
        #         # letters = re.findall(r'[a-zA-Z]+', expr)
        #         # unit = letters[0] if letters else 'day'
        #         return f"{this} + INTERVAL {expr} DAY"
        #     return self.binary(expression, "+")

        def where_sql(self, expression: exp.Where) -> str:
            this = self.indent(self.sql(expression, "this"))
            numbers = re.findall(r"\d+", this)
            letters = re.findall(r"[a-zA-Z]+", this)
            if letters[0] == "rownum":
                return f"{self.seg('LIMIT')} {numbers[0]}"
            return f"{self.seg('WHERE')}{self.sep()}{this}"

        def currentdate_sql(self, expression: exp.CurrentDate) -> str:
            zone = self.sql(expression, "this")
            return f"CURRENT_DATE({zone})" if zone else "CURRENT_DATE()"
