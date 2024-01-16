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
            exp.ArrayUniq: lambda self, e: f"SIZE(ARRAY_DISTINCT({self.sql(e, 'this')}))",
            exp.ArrayOverlaps: rename_func("ARRAYS_OVERLAP"),
            exp.BitwiseNot: rename_func("BITNOT"),
            exp.BitwiseAnd: rename_func("BITAND"),
            exp.BitwiseOr: rename_func("BITOR"),
            exp.BitwiseXor: rename_func("BITXOR"),
            exp.ArrayStringConcat: handle_array_concat,
            exp.ArrayToString: handle_array_to_string,
            exp.ArrayUniqueAgg: rename_func("COLLECT_SET"),
            exp.CurrentTimestamp: lambda *_: "NOW()",
            exp.CurrentDate: no_paren_current_date_sql,
            exp.CountIf: count_if_to_sum,
            exp.DateTrunc: lambda self, e: self.func(
                "DATE_TRUNC", e.this, "'" + e.text("unit") + "'"
            ),
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
