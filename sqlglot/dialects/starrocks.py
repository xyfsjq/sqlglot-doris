from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import (
    approx_count_distinct_sql,
    arrow_json_extract_sql,
    rename_func,
)
from sqlglot.dialects.mysql import MySQL
from sqlglot.helper import seq_get


class StarRocks(MySQL):
    class Parser(MySQL.Parser):
        FUNCTIONS = {
            **MySQL.Parser.FUNCTIONS,
            "DATEDIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 0), expression=seq_get(args, 1), unit=exp.Literal.string("DAY")
            ),
            "DATE_DIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 1), expression=seq_get(args, 2), unit=seq_get(args, 0)
            ),
            "REGEXP": exp.RegexpLike.from_arg_list,
            "ADD_MONTHS": exp.MonthsAdd.from_arg_list,
            "ADDDATE": exp.TsOrDsAdd.from_arg_list,
            "DATE": exp.TimeStrToDate.from_arg_list,
            "DATE_TRUNC": exp.DateTrunc.from_arg_list,
            # "MICROSECONDS_SUB": lambda args: exp.MICROSECONDS_ADD(
            #     this=seq_get(args, 0), expression=-seq_get(args, 1)
            # ),
            "STR2DATE": exp.StrToDate.from_arg_list,
            "JSON_EXISTS": exp.JSON_EXISTS_PATH.from_arg_list,
            "JSON_QUERY": exp.JSONExtract.from_arg_list,
            "LIKE": exp.RegexpLike.from_arg_list,
            "MULTI_DISTINCT_COUNT": exp.MULTI_DISTINCT_COUNT.from_arg_list,
            "MULTI_DISTINCT_SUM": exp.MULTI_DISTINCT_SUM.from_arg_list,
            "ARRAY_TO_BITMAP": exp.BitmapFromArray.from_arg_list,
            "UNNEST": exp.Explode.from_arg_list,
            "PERCENTILE_APPROX_RAW": exp.PERCENTILE_APPROX.from_arg_list,
            # "RETENTION": lambda args: exp.RETENTION(this=", ".join(map(str, seq_get(args, 0)))),
        }

    class Generator(MySQL.Generator):
        CAST_MAPPING = {}

        TYPE_MAPPING = {
            **MySQL.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.TIMESTAMP: "DATETIME",
            exp.DataType.Type.TIMESTAMPTZ: "DATETIME",
        }

        TRANSFORMS = {
            **MySQL.Generator.TRANSFORMS,
            exp.ApproxDistinct: approx_count_distinct_sql,
            exp.DateDiff: lambda self, e: self.func(
                "DATE_DIFF", exp.Literal.string(e.text("unit") or "DAY"), e.this, e.expression
            ),
            exp.JSONExtractScalar: arrow_json_extract_sql,
            exp.JSONExtract: arrow_json_extract_sql,
            exp.RegexpLike: rename_func("REGEXP"),
            exp.StrToUnix: lambda self, e: f"UNIX_TIMESTAMP({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.TimestampTrunc: lambda self, e: self.func(
                "DATE_TRUNC", exp.Literal.string(e.text("unit")), e.this
            ),
            exp.TimeStrToDate: rename_func("TO_DATE"),
            exp.UnixToStr: lambda self, e: f"FROM_UNIXTIME({self.sql(e, 'this')}, {self.format_time(e)})",
            exp.UnixToTime: rename_func("FROM_UNIXTIME"),
        }

        TRANSFORMS.pop(exp.DateTrunc)
