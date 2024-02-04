from __future__ import annotations

import typing as t

from sqlglot import exp, generator, parser, tokens, transforms
from sqlglot.dialects.dialect import (
    Dialect,
    arg_max_or_min_no_count,
    date_delta_sql,
    inline_array_sql,
    no_pivot_sql,
    parse_json_extract_string,
    rename_func,
    var_map_sql,
)
from sqlglot.errors import ParseError
from sqlglot.helper import seq_get
from sqlglot.parser import parse_var_map
from sqlglot.tokens import Token, TokenType


def _lower_func(sql: str) -> str:
    index = sql.index("(")
    return sql[:index].lower() + sql[index:]


def _quantile_sql(self: ClickHouse.Generator, e: exp.Quantile) -> str:
    quantile = e.args["quantile"]
    args = f"({self.sql(e, 'this')})"

    if isinstance(quantile, exp.Array):
        func = self.func("quantiles", *quantile)
    else:
        func = self.func("quantile", quantile)

    return func + args


def _parse_count_if(args: t.List) -> exp.CountIf | exp.CombinedAggFunc:
    if len(args) == 1:
        return exp.CountIf(this=seq_get(args, 0))

    return exp.CombinedAggFunc(this="countIf", expressions=args, parts=("count", "If"))


class ClickHouse(Dialect):
    NORMALIZE_FUNCTIONS: bool | str = False
    NULL_ORDERING = "nulls_are_last"
    SUPPORTS_USER_DEFINED_TYPES = False
    SAFE_DIVISION = True

    ESCAPE_SEQUENCES = {
        "\\0": "\0",
    }

    class Tokenizer(tokens.Tokenizer):
        COMMENTS = ["--", "#", "#!", ("/*", "*/")]
        IDENTIFIERS = ['"', "`"]
        STRING_ESCAPES = ["'", "\\"]
        BIT_STRINGS = [("0b", "")]
        HEX_STRINGS = [("0x", ""), ("0X", "")]
        HEREDOC_STRINGS = ["$"]

        KEYWORDS = {
            **tokens.Tokenizer.KEYWORDS,
            "ATTACH": TokenType.COMMAND,
            "DATE32": TokenType.DATE32,
            "DATETIME64": TokenType.DATETIME64,
            "DICTIONARY": TokenType.DICTIONARY,
            "ENUM": TokenType.ENUM,
            "ENUM8": TokenType.ENUM8,
            "ENUM16": TokenType.ENUM16,
            "FINAL": TokenType.FINAL,
            "FIXEDSTRING": TokenType.FIXEDSTRING,
            "FLOAT32": TokenType.FLOAT,
            "FLOAT64": TokenType.DOUBLE,
            "GLOBAL": TokenType.GLOBAL,
            "INT256": TokenType.INT256,
            "LOWCARDINALITY": TokenType.LOWCARDINALITY,
            "MAP": TokenType.MAP,
            "NESTED": TokenType.NESTED,
            "SAMPLE": TokenType.TABLE_SAMPLE,
            "TUPLE": TokenType.STRUCT,
            "UINT128": TokenType.UINT128,
            "UINT16": TokenType.USMALLINT,
            "UINT256": TokenType.UINT256,
            "UINT32": TokenType.UINT,
            "UINT64": TokenType.UBIGINT,
            "UINT8": TokenType.UTINYINT,
            "IPV4": TokenType.IPV4,
            "IPV6": TokenType.IPV6,
            "AGGREGATEFUNCTION": TokenType.AGGREGATEFUNCTION,
            "SIMPLEAGGREGATEFUNCTION": TokenType.SIMPLEAGGREGATEFUNCTION,
        }

        SINGLE_TOKENS = {
            **tokens.Tokenizer.SINGLE_TOKENS,
            "$": TokenType.HEREDOC_STRING,
        }

    class Parser(parser.Parser):
        # Tested in ClickHouse's playground, it seems that the following two queries do the same thing
        # * select x from t1 union all select x from t2 limit 1;
        # * select x from t1 union all (select x from t2 limit 1);
        MODIFIERS_ATTACHED_TO_UNION = False

        FUNCTIONS = {
            **parser.Parser.FUNCTIONS,
            "ADDYEARS": exp.YearsAdd.from_arg_list,
            "ADDMONTHS": exp.MonthsAdd.from_arg_list,
            "ADDWEEKS": exp.WeeksAdd.from_arg_list,
            "ADDDAYS": exp.DaysAdd.from_arg_list,
            "ADDHOURS": exp.HoursAdd.from_arg_list,
            "ADDMINUTES": exp.MinutesAdd.from_arg_list,
            "ADDSECONDS": exp.SecondsAdd.from_arg_list,
            "ADDQUARTERS": exp.QuartersAdd.from_arg_list,
            "ANY": exp.AnyValue.from_arg_list,
            "ARRAYAVG": exp.ArrayAvg.from_arg_list,
            "ARRAYCOMPACT": exp.ArrayDistinct.from_arg_list,
            "ARRAYCUMSUM": exp.ArrayCumSum.from_arg_list,
            "ARRAYDIFFERENCE": exp.ArrayDifference.from_arg_list,
            "ARRAYDISTINCT": exp.ArrayDistinct.from_arg_list,
            "ARRAYELEMENT": exp.ArrayPosition.from_arg_list,
            "ARRAYEXISTS": exp.ArrayExists.from_arg_list,
            "ARRAYFILTER": lambda args: exp.ArrayFilter(
                this=seq_get(args, 1), expression=seq_get(args, 0)
            ),
            "ARRAYFIRST": exp.ArrayFirst.from_arg_list,
            "ARRAYFIRSTINDEX": exp.ArrayFirstIndex.from_arg_list,
            "ARRAYINTERSECT": exp.ArrayIntersect.from_arg_list,
            "ARRAYLAST": exp.ArrayLast.from_arg_list,
            "ARRAYLASTINDEX": exp.ArrayLastIndex.from_arg_list,
            "ARRAYMAP": exp.ArrayMap.from_arg_list,
            "ARRAYPRODUCT": exp.ArrayProduct.from_arg_list,
            "ARRAYPOPBACK": exp.ArrayPopback.from_arg_list,
            "ARRAYPOPFRONT": exp.ArrayPopfront.from_arg_list,
            "ARRAYPUSHBACK": exp.ArrayPushback.from_arg_list,
            "ARRAYPUSHFRONT": exp.ArrayPushfront.from_arg_list,
            "ARRAYSLICE": exp.ArraySlice.from_arg_list,
            "ARRAYREVERSESORT": exp.ArrayReverseSort.from_arg_list,
            "ARRAYSORT": exp.SortArray.from_arg_list,
            "ARRAYSTRINGCONCAT": exp.ArrayStringConcat.from_arg_list,
            "ARRAYSUM": exp.ArraySum.from_arg_list,
            "ARRAYUNIQ": exp.ArrayUniq.from_arg_list,
            "ARRAYZIP": exp.ArrayZip.from_arg_list,
            "BITMAPAND": exp.BitmapAnd.from_arg_list,
            "BITMAPANDCARDINALITY": exp.BitmapAndCount.from_arg_list,
            "BITMAPANDNOT": exp.BitmapAndNot.from_arg_list,
            "BITMAPANDNOTCARDINALITY": exp.BitmapAndNotCount.from_arg_list,
            "BITMAPBUILD": exp.BitmapFromArray.from_arg_list,
            "BITMAPCARDINALITY": exp.BitmapCount.from_arg_list,
            "BITMAPCONTAINS": exp.BitmapContains.from_arg_list,
            "BITMAPOR": exp.BitmapOr.from_arg_list,
            "BITMAPORCARDINALITY": exp.BitmapOrCount.from_arg_list,
            "BITMAPXOR": exp.BitmapXor.from_arg_list,
            "BITMAPXORCARDINALITY": exp.BitmapXOrCount.from_arg_list,
            "BITMAPHASALL": exp.BitmapHasAll.from_arg_list,
            "BITMAPHASANY": exp.BitmapHasAny.from_arg_list,
            "BITMAPTOARRAY": exp.BitmapToArray.from_arg_list,
            "COUNTIF": _parse_count_if,
            "BASE64ENCODE": exp.ToBase64.from_arg_list,
            "BASE64DECODE": exp.FromBase64.from_arg_list,
            "DATE_ADD": lambda args: exp.DateAdd(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DATEADD": lambda args: exp.DateAdd(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DATE_DIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DATEDIFF": lambda args: exp.DateDiff(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DATE_SUB": lambda args: exp.DateSub(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "DATESUB": lambda args: exp.DateSub(
                this=seq_get(args, 2), expression=seq_get(args, 1), unit=seq_get(args, 0)
            ),
            "EMPTY": exp.Empty.from_arg_list,
            "ENDSWITH": exp.EndsWith.from_arg_list,
            "EXP2": lambda args: exp.Pow(
                this="2",
                expression=seq_get(args, 0),
            ),
            "EXP10": lambda args: exp.Pow(
                this="10",
                expression=seq_get(args, 0),
            ),
            "EXTRACTALL": exp.RegexpExtract.from_arg_list,
            "GROUPARRAY": exp.ArrayAgg.from_arg_list,
            "GROUPBITAND": exp.GroupBitAnd.from_arg_list,
            "GROUPBITOR": exp.GroupBitOr.from_arg_list,
            "GROUPBITXOR": exp.GroupBitXor.from_arg_list,
            "HAS": exp.ArrayContains.from_arg_list,
            "IPV4STRINGTONUMORDEFAULT": exp.Ipv4StringToNumOrDefault.from_arg_list,
            "IPV6STRINGTONUMORDEFAULT": exp.Ipv6StringToNumOrDefault.from_arg_list,
            "ISIPV4STRING": exp.IsIpv4String.from_arg_list,
            "ISIPV6STRING": exp.IsIpv6String.from_arg_list,
            "JSONEXTRACTSTRING": parse_json_extract_string(exp.JSONExtract),
            "LENGTHUTF8": exp.CharLength.from_arg_list,
            "LOWERUTF8": exp.Lower.from_arg_list,
            "MAP": parse_var_map,
            "MATCH": exp.RegexpLike.from_arg_list,
            "MID": exp.Substring.from_arg_list,
            "MULTIMATCHANY": exp.MultiMatchAny.from_arg_list,
            "NOTEMPTY": exp.NotEmpty.from_arg_list,
            "RANDCANONICAL": exp.Rand.from_arg_list,
            "RANGE": exp.ArrayRange.from_arg_list,
            "REPLACEALL": exp.Replace.from_arg_list,
            "REPLACEREGEXPONE": exp.RegexpReplaceOne.from_arg_list,
            "REPLACEREGEXPALL": exp.RegexpReplace.from_arg_list,
            "POSITIONUTF8": exp.StrPosition.from_arg_list,
            "SPLITBYCHAR": lambda args: exp.Split(
                this=seq_get(args, 1),
                expression=seq_get(args, 0),
            ),
            "SPLITBYSTRING": lambda args: exp.Split(
                this=seq_get(args, 1),
                expression=seq_get(args, 0),
            ),
            "STARTSWITH": exp.StartsWith.from_arg_list,
            "STDDEVPOP": exp.StddevPop.from_arg_list,
            "STDDEVSAMP": exp.StddevSamp.from_arg_list,
            "SUBSTRINGUTF8": exp.Substring.from_arg_list,
            "SUBTRACTYEARS": exp.YearsSub.from_arg_list,
            "SUBTRACTMONTHS": exp.MonthsSub.from_arg_list,
            "SUBTRACTSECONDS": exp.SecondsSub.from_arg_list,
            "SUBTRACTQUARTERS": exp.QuartersSub.from_arg_list,
            "TODAY": exp.Today.from_arg_list,
            "TODATE": exp.TimeStrToDate.from_arg_list,
            "TODATETIME": lambda args: exp.CastToStrType(
                this=seq_get(args, 0),
                to="DATETIME",
            ),
            "TOHOUR": exp.Hour.from_arg_list,
            "TOMONTH": exp.Month.from_arg_list,
            "TOMINUTE": exp.Minute.from_arg_list,
            "TOQUARTER": exp.Quarter.from_arg_list,
            "TOSTRING": lambda args: exp.CastToStrType(
                this=seq_get(args, 0),
                to="STRING",
            ),
            "TOSTARTOFQUARTER": exp.ToStartOfQuarter.from_arg_list,
            "TOSTARTOFMONTH": exp.ToStartOfMonth.from_arg_list,
            "TOSTARTOFWEEK": exp.ToStartOfWeek.from_arg_list,
            "TOSTARTOFDAY": exp.ToStartOfDay.from_arg_list,
            "TOSTARTOFHOUR": exp.ToStartOfHour.from_arg_list,
            "TOSTARTOFMINUTE": exp.ToStartOfMinute.from_arg_list,
            "TOSTARTOFSECOND": exp.ToStartOfSecond.from_arg_list,
            "TOSECOND": exp.Second.from_arg_list,
            "TOUNIXTIMESTAMP": exp.TimeToUnix.from_arg_list,
            "TOYEAR": exp.Year.from_arg_list,
            "TOYYYYMM": exp.ToYyyymm.from_arg_list,
            "TOYYYYMMDD": exp.ToYyyymmdd.from_arg_list,
            "TOYYYYMMDDHHMMSS": exp.ToYyyymmddhhmmss.from_arg_list,
            "TRIMLEFT": exp.LTrim.from_arg_list,
            "TRIMRIGHT": exp.RTrim.from_arg_list,
            "UNIQ": exp.ApproxDistinct.from_arg_list,
            "UPPERUTF8": exp.Upper.from_arg_list,
            "VARPOP": exp.VariancePop.from_arg_list,
            "VARSAMP": exp.Variance.from_arg_list,
            "XOR": lambda args: exp.Xor(expressions=args),
        }

        AGG_FUNCTIONS = {
            "count",
            "min",
            "max",
            "sum",
            "avg",
            "any",
            "stddevPop",
            "stddevSamp",
            "varPop",
            "varSamp",
            "corr",
            "covarPop",
            "covarSamp",
            "entropy",
            "exponentialMovingAverage",
            "intervalLengthSum",
            "kolmogorovSmirnovTest",
            "mannWhitneyUTest",
            "median",
            "rankCorr",
            "sumKahan",
            "studentTTest",
            "welchTTest",
            "anyHeavy",
            "anyLast",
            "boundingRatio",
            "first_value",
            "last_value",
            "argMin",
            "argMax",
            "avgWeighted",
            "topK",
            "topKWeighted",
            "deltaSum",
            "deltaSumTimestamp",
            "groupArray",
            "groupArrayLast",
            "groupUniqArray",
            "groupArrayInsertAt",
            "groupArrayMovingAvg",
            "groupArrayMovingSum",
            "groupArraySample",
            "groupBitAnd",
            "groupBitOr",
            "groupBitXor",
            "groupBitmap",
            "groupBitmapAnd",
            "groupBitmapOr",
            "groupBitmapXor",
            "sumWithOverflow",
            "sumMap",
            "minMap",
            "maxMap",
            "skewSamp",
            "skewPop",
            "kurtSamp",
            "kurtPop",
            "uniq",
            "uniqExact",
            "uniqCombined",
            "uniqCombined64",
            "uniqHLL12",
            "uniqTheta",
            "quantile",
            "quantiles",
            "quantileExact",
            "quantilesExact",
            "quantileExactLow",
            "quantilesExactLow",
            "quantileExactHigh",
            "quantilesExactHigh",
            "quantileExactWeighted",
            "quantilesExactWeighted",
            "quantileTiming",
            "quantilesTiming",
            "quantileTimingWeighted",
            "quantilesTimingWeighted",
            "quantileDeterministic",
            "quantilesDeterministic",
            "quantileTDigest",
            "quantilesTDigest",
            "quantileTDigestWeighted",
            "quantilesTDigestWeighted",
            "quantileBFloat16",
            "quantilesBFloat16",
            "quantileBFloat16Weighted",
            "quantilesBFloat16Weighted",
            "simpleLinearRegression",
            "stochasticLinearRegression",
            "stochasticLogisticRegression",
            "categoricalInformationValue",
            "contingency",
            "cramersV",
            "cramersVBiasCorrected",
            "theilsU",
            "maxIntersections",
            "maxIntersectionsPosition",
            "meanZTest",
            "quantileInterpolatedWeighted",
            "quantilesInterpolatedWeighted",
            "quantileGK",
            "quantilesGK",
            "sparkBar",
            "sumCount",
            "largestTriangleThreeBuckets",
        }

        AGG_FUNCTIONS_SUFFIXES = [
            "If",
            "Array",
            "ArrayIf",
            "Map",
            "SimpleState",
            "State",
            "Merge",
            "MergeState",
            "ForEach",
            "Distinct",
            "OrDefault",
            "OrNull",
            "Resample",
            "ArgMin",
            "ArgMax",
        ]

        AGG_FUNC_MAPPING = (
            lambda functions, suffixes: {
                f"{f}{sfx}": (f, sfx) for sfx in (suffixes + [""]) for f in functions
            }
        )(AGG_FUNCTIONS, AGG_FUNCTIONS_SUFFIXES)

        FUNCTIONS_WITH_ALIASED_ARGS = {*parser.Parser.FUNCTIONS_WITH_ALIASED_ARGS, "TUPLE"}

        FUNCTION_PARSERS = {
            **parser.Parser.FUNCTION_PARSERS,
            "ARRAYJOIN": lambda self: self.expression(exp.Explode, this=self._parse_expression()),
            "QUANTILE": lambda self: self._parse_quantile(),
        }

        FUNCTION_PARSERS.pop("MATCH")

        NO_PAREN_FUNCTION_PARSERS = parser.Parser.NO_PAREN_FUNCTION_PARSERS.copy()
        NO_PAREN_FUNCTION_PARSERS.pop("ANY")

        RANGE_PARSERS = {
            **parser.Parser.RANGE_PARSERS,
            TokenType.GLOBAL: lambda self, this: self._match(TokenType.IN)
            and self._parse_in(this, is_global=True),
        }

        # The PLACEHOLDER entry is popped because 1) it doesn't affect Clickhouse (it corresponds to
        # the postgres-specific JSONBContains parser) and 2) it makes parsing the ternary op simpler.
        COLUMN_OPERATORS = parser.Parser.COLUMN_OPERATORS.copy()
        COLUMN_OPERATORS.pop(TokenType.PLACEHOLDER)

        JOIN_KINDS = {
            *parser.Parser.JOIN_KINDS,
            TokenType.ANY,
            TokenType.ASOF,
            TokenType.ARRAY,
        }

        TABLE_ALIAS_TOKENS = parser.Parser.TABLE_ALIAS_TOKENS - {
            TokenType.ANY,
            TokenType.ARRAY,
            TokenType.FINAL,
            TokenType.FORMAT,
            TokenType.SETTINGS,
        }

        LOG_DEFAULTS_TO_LN = True

        QUERY_MODIFIER_PARSERS = {
            **parser.Parser.QUERY_MODIFIER_PARSERS,
            TokenType.SETTINGS: lambda self: (
                "settings",
                self._advance() or self._parse_csv(self._parse_conjunction),
            ),
            TokenType.FORMAT: lambda self: ("format", self._advance() or self._parse_id_var()),
        }

        def _parse_conjunction(self) -> t.Optional[exp.Expression]:
            this = super()._parse_conjunction()

            if self._match(TokenType.PLACEHOLDER):
                return self.expression(
                    exp.If,
                    this=this,
                    true=self._parse_conjunction(),
                    false=self._match(TokenType.COLON) and self._parse_conjunction(),
                )

            return this

        def _parse_placeholder(self) -> t.Optional[exp.Expression]:
            """
            Parse a placeholder expression like SELECT {abc: UInt32} or FROM {table: Identifier}
            https://clickhouse.com/docs/en/sql-reference/syntax#defining-and-using-query-parameters
            """
            if not self._match(TokenType.L_BRACE):
                return None

            this = self._parse_id_var()
            self._match(TokenType.COLON)
            kind = self._parse_types(check_func=False, allow_identifiers=False) or (
                self._match_text_seq("IDENTIFIER") and "Identifier"
            )

            if not kind:
                self.raise_error("Expecting a placeholder type or 'Identifier' for tables")
            elif not self._match(TokenType.R_BRACE):
                self.raise_error("Expecting }")

            return self.expression(exp.Placeholder, this=this, kind=kind)

        def _parse_in(self, this: t.Optional[exp.Expression], is_global: bool = False) -> exp.In:
            this = super()._parse_in(this)
            this.set("is_global", is_global)
            return this

        def _parse_table(
            self,
            schema: bool = False,
            joins: bool = False,
            alias_tokens: t.Optional[t.Collection[TokenType]] = None,
            parse_bracket: bool = False,
        ) -> t.Optional[exp.Expression]:
            this = super()._parse_table(
                schema=schema, joins=joins, alias_tokens=alias_tokens, parse_bracket=parse_bracket
            )

            if self._match(TokenType.FINAL):
                this = self.expression(exp.Final, this=this)

            return this

        def _parse_position(self, haystack_first: bool = False) -> exp.StrPosition:
            return super()._parse_position(haystack_first=True)

        # https://clickhouse.com/docs/en/sql-reference/statements/select/with/
        def _parse_cte(self) -> exp.CTE:
            index = self._index
            try:
                # WITH <identifier> AS <subquery expression>
                return super()._parse_cte()
            except ParseError:
                # WITH <expression> AS <identifier>
                self._retreat(index)

                return self.expression(
                    exp.CTE,
                    this=self._parse_field(),
                    alias=self._parse_table_alias(),
                    scalar=True,
                )

        def _parse_join_parts(
            self,
        ) -> t.Tuple[t.Optional[Token], t.Optional[Token], t.Optional[Token]]:
            is_global = self._match(TokenType.GLOBAL) and self._prev
            kind_pre = self._match_set(self.JOIN_KINDS, advance=False) and self._prev

            if kind_pre:
                kind = self._match_set(self.JOIN_KINDS) and self._prev
                side = self._match_set(self.JOIN_SIDES) and self._prev
                return is_global, side, kind

            return (
                is_global,
                self._match_set(self.JOIN_SIDES) and self._prev,
                self._match_set(self.JOIN_KINDS) and self._prev,
            )

        def _parse_join(
            self, skip_join_token: bool = False, parse_bracket: bool = False
        ) -> t.Optional[exp.Join]:
            join = super()._parse_join(skip_join_token=skip_join_token, parse_bracket=True)

            if join:
                join.set("global", join.args.pop("method", None))
            return join

        def _parse_function(
            self,
            functions: t.Optional[t.Dict[str, t.Callable]] = None,
            anonymous: bool = False,
            optional_parens: bool = True,
        ) -> t.Optional[exp.Expression]:
            func = super()._parse_function(
                functions=functions, anonymous=anonymous, optional_parens=optional_parens
            )

            if isinstance(func, exp.Anonymous):
                parts = self.AGG_FUNC_MAPPING.get(func.this)
                params = self._parse_func_params(func)

                if params:
                    if parts and parts[1]:
                        return self.expression(
                            exp.CombinedParameterizedAgg,
                            this=func.this,
                            expressions=func.expressions,
                            params=params,
                            parts=parts,
                        )
                    return self.expression(
                        exp.ParameterizedAgg,
                        this=func.this,
                        expressions=func.expressions,
                        params=params,
                    )

                if parts:
                    if parts[1]:
                        return self.expression(
                            exp.CombinedAggFunc,
                            this=func.this,
                            expressions=func.expressions,
                            parts=parts,
                        )
                    return self.expression(
                        exp.AnonymousAggFunc,
                        this=func.this,
                        expressions=func.expressions,
                    )

            return func

        def _parse_func_params(
            self, this: t.Optional[exp.Func] = None
        ) -> t.Optional[t.List[exp.Expression]]:
            if self._match_pair(TokenType.R_PAREN, TokenType.L_PAREN):
                return self._parse_csv(self._parse_lambda)

            if self._match(TokenType.L_PAREN):
                params = self._parse_csv(self._parse_lambda)
                self._match_r_paren(this)
                return params

            return None

        def _parse_quantile(self) -> exp.Quantile:
            this = self._parse_lambda()
            params = self._parse_func_params()
            if params:
                return self.expression(exp.Quantile, this=params[0], quantile=this)
            return self.expression(exp.Quantile, this=this, quantile=exp.Literal.number(0.5))

        def _parse_wrapped_id_vars(self, optional: bool = False) -> t.List[exp.Expression]:
            return super()._parse_wrapped_id_vars(optional=True)

        def _parse_primary_key(
            self, wrapped_optional: bool = False, in_props: bool = False
        ) -> exp.PrimaryKeyColumnConstraint | exp.PrimaryKey:
            return super()._parse_primary_key(
                wrapped_optional=wrapped_optional or in_props, in_props=in_props
            )

        def _parse_on_property(self) -> t.Optional[exp.Expression]:
            index = self._index
            if self._match_text_seq("CLUSTER"):
                this = self._parse_id_var()
                if this:
                    return self.expression(exp.OnCluster, this=this)
                else:
                    self._retreat(index)
            return None

    class Generator(generator.Generator):
        QUERY_HINTS = False
        STRUCT_DELIMITER = ("(", ")")
        NVL2_SUPPORTED = False
        TABLESAMPLE_REQUIRES_PARENS = False
        TABLESAMPLE_SIZE_IS_ROWS = False
        TABLESAMPLE_KEYWORDS = "SAMPLE"
        LAST_DAY_SUPPORTS_DATE_PART = False

        STRING_TYPE_MAPPING = {
            exp.DataType.Type.CHAR: "String",
            exp.DataType.Type.LONGBLOB: "String",
            exp.DataType.Type.LONGTEXT: "String",
            exp.DataType.Type.MEDIUMBLOB: "String",
            exp.DataType.Type.MEDIUMTEXT: "String",
            exp.DataType.Type.TINYBLOB: "String",
            exp.DataType.Type.TINYTEXT: "String",
            exp.DataType.Type.TEXT: "String",
            exp.DataType.Type.VARBINARY: "String",
            exp.DataType.Type.VARCHAR: "String",
        }

        TYPE_MAPPING = {
            **generator.Generator.TYPE_MAPPING,
            **STRING_TYPE_MAPPING,
            exp.DataType.Type.ARRAY: "Array",
            exp.DataType.Type.BIGINT: "Int64",
            exp.DataType.Type.DATE32: "Date32",
            exp.DataType.Type.DATETIME64: "DateTime64",
            exp.DataType.Type.DOUBLE: "Float64",
            exp.DataType.Type.ENUM: "Enum",
            exp.DataType.Type.ENUM8: "Enum8",
            exp.DataType.Type.ENUM16: "Enum16",
            exp.DataType.Type.FIXEDSTRING: "FixedString",
            exp.DataType.Type.FLOAT: "Float32",
            exp.DataType.Type.INT: "Int32",
            exp.DataType.Type.MEDIUMINT: "Int32",
            exp.DataType.Type.INT128: "Int128",
            exp.DataType.Type.INT256: "Int256",
            exp.DataType.Type.LOWCARDINALITY: "LowCardinality",
            exp.DataType.Type.MAP: "Map",
            exp.DataType.Type.NESTED: "Nested",
            exp.DataType.Type.NULLABLE: "Nullable",
            exp.DataType.Type.SMALLINT: "Int16",
            exp.DataType.Type.STRUCT: "Tuple",
            exp.DataType.Type.TINYINT: "Int8",
            exp.DataType.Type.UBIGINT: "UInt64",
            exp.DataType.Type.UINT: "UInt32",
            exp.DataType.Type.UINT128: "UInt128",
            exp.DataType.Type.UINT256: "UInt256",
            exp.DataType.Type.USMALLINT: "UInt16",
            exp.DataType.Type.UTINYINT: "UInt8",
            exp.DataType.Type.IPV4: "IPv4",
            exp.DataType.Type.IPV6: "IPv6",
            exp.DataType.Type.AGGREGATEFUNCTION: "AggregateFunction",
            exp.DataType.Type.SIMPLEAGGREGATEFUNCTION: "SimpleAggregateFunction",
        }

        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.AnyValue: rename_func("any"),
            exp.ApproxDistinct: rename_func("uniq"),
            exp.ArraySum: rename_func("arraySum"),
            exp.ArgMax: arg_max_or_min_no_count("argMax"),
            exp.ArgMin: arg_max_or_min_no_count("argMin"),
            exp.Array: inline_array_sql,
            exp.CastToStrType: rename_func("CAST"),
            exp.CountIf: rename_func("countIf"),
            exp.CurrentDate: lambda self, e: self.func("CURRENT_DATE"),
            exp.DateAdd: date_delta_sql("DATE_ADD"),
            exp.DateDiff: date_delta_sql("DATE_DIFF"),
            exp.Explode: rename_func("arrayJoin"),
            exp.Final: lambda self, e: f"{self.sql(e, 'this')} FINAL",
            exp.IsNan: rename_func("isNaN"),
            exp.Map: lambda self, e: _lower_func(var_map_sql(self, e)),
            exp.Nullif: rename_func("nullIf"),
            exp.PartitionedByProperty: lambda self, e: f"PARTITION BY {self.sql(e, 'this')}",
            exp.Pivot: no_pivot_sql,
            exp.Quantile: _quantile_sql,
            exp.RegexpLike: lambda self, e: f"match({self.format_args(e.this, e.expression)})",
            exp.Rand: rename_func("randCanonical"),
            exp.Select: transforms.preprocess([transforms.eliminate_qualify]),
            exp.StartsWith: rename_func("startsWith"),
            exp.StrPosition: lambda self,
            e: f"position({self.format_args(e.this, e.args.get('substr'), e.args.get('position'))})",
            exp.VarMap: lambda self, e: _lower_func(var_map_sql(self, e)),
            exp.Xor: lambda self, e: self.func("xor", e.this, e.expression, *e.expressions),
        }

        PROPERTIES_LOCATION = {
            **generator.Generator.PROPERTIES_LOCATION,
            exp.VolatileProperty: exp.Properties.Location.UNSUPPORTED,
            exp.PartitionedByProperty: exp.Properties.Location.POST_SCHEMA,
            exp.OnCluster: exp.Properties.Location.POST_NAME,
        }

        JOIN_HINTS = False
        TABLE_HINTS = False
        EXPLICIT_UNION = True
        GROUPINGS_SEP = ""

        # there's no list in docs, but it can be found in Clickhouse code
        # see `ClickHouse/src/Parsers/ParserCreate*.cpp`
        ON_CLUSTER_TARGETS = {
            "DATABASE",
            "TABLE",
            "VIEW",
            "DICTIONARY",
            "INDEX",
            "FUNCTION",
            "NAMED COLLECTION",
        }

        def _any_to_has(
            self,
            expression: exp.EQ | exp.NEQ,
            default: t.Callable[[t.Any], str],
            prefix: str = "",
        ) -> str:
            if isinstance(expression.left, exp.Any):
                arr = expression.left
                this = expression.right
            elif isinstance(expression.right, exp.Any):
                arr = expression.right
                this = expression.left
            else:
                return default(expression)
            return prefix + self.func("has", arr.this.unnest(), this)

        def eq_sql(self, expression: exp.EQ) -> str:
            return self._any_to_has(expression, super().eq_sql)

        def neq_sql(self, expression: exp.NEQ) -> str:
            return self._any_to_has(expression, super().neq_sql, "NOT ")

        def regexpilike_sql(self, expression: exp.RegexpILike) -> str:
            # Manually add a flag to make the search case-insensitive
            regex = self.func("CONCAT", "'(?i)'", expression.expression)
            return f"match({self.format_args(expression.this, regex)})"

        def datatype_sql(self, expression: exp.DataType) -> str:
            # String is the standard ClickHouse type, every other variant is just an alias.
            # Additionally, any supplied length parameter will be ignored.
            #
            # https://clickhouse.com/docs/en/sql-reference/data-types/string
            if expression.this in self.STRING_TYPE_MAPPING:
                return "String"

            return super().datatype_sql(expression)

        def cte_sql(self, expression: exp.CTE) -> str:
            if expression.args.get("scalar"):
                this = self.sql(expression, "this")
                alias = self.sql(expression, "alias")
                return f"{this} AS {alias}"

            return super().cte_sql(expression)

        def after_limit_modifiers(self, expression: exp.Expression) -> t.List[str]:
            return super().after_limit_modifiers(expression) + [
                (
                    self.seg("SETTINGS ") + self.expressions(expression, key="settings", flat=True)
                    if expression.args.get("settings")
                    else ""
                ),
                (
                    self.seg("FORMAT ") + self.sql(expression, "format")
                    if expression.args.get("format")
                    else ""
                ),
            ]

        def parameterizedagg_sql(self, expression: exp.ParameterizedAgg) -> str:
            params = self.expressions(expression, key="params", flat=True)
            return self.func(expression.name, *expression.expressions) + f"({params})"

        def anonymousaggfunc_sql(self, expression: exp.AnonymousAggFunc) -> str:
            return self.func(expression.name, *expression.expressions)

        def combinedaggfunc_sql(self, expression: exp.CombinedAggFunc) -> str:
            return self.anonymousaggfunc_sql(expression)

        def combinedparameterizedagg_sql(self, expression: exp.CombinedParameterizedAgg) -> str:
            return self.parameterizedagg_sql(expression)

        def placeholder_sql(self, expression: exp.Placeholder) -> str:
            return f"{{{expression.name}: {self.sql(expression, 'kind')}}}"

        def oncluster_sql(self, expression: exp.OnCluster) -> str:
            return f"ON CLUSTER {self.sql(expression, 'this')}"

        def createable_sql(self, expression: exp.Create, locations: t.DefaultDict) -> str:
            kind = self.sql(expression, "kind").upper()
            if kind in self.ON_CLUSTER_TARGETS and locations.get(exp.Properties.Location.POST_NAME):
                this_name = self.sql(expression.this, "this")
                this_properties = " ".join(
                    [self.sql(prop) for prop in locations[exp.Properties.Location.POST_NAME]]
                )
                this_schema = self.schema_columns_sql(expression.this)
                return f"{this_name}{self.sep()}{this_properties}{self.sep()}{this_schema}"

            return super().createable_sql(expression, locations)
