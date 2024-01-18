from tests.dialects.test_dialect import Validator


class TestDoris(Validator):
    dialect = "doris"

    def test_doris(self):
        self.validate_all(
            "SELECT TO_DATE('2020-02-02 00:00:00')",
            write={
                "doris": "SELECT TO_DATE('2020-02-02 00:00:00')",
                "oracle": "SELECT CAST('2020-02-02 00:00:00' AS DATE)",
            },
        )
        self.validate_all(
            "SELECT Approx_Quantile(x,1)",
            write={
                "doris": "SELECT PERCENTILE_APPROX(x, 1)",
            },
        )
        self.validate_all(
            "SELECT PERCENTILE_APPROX(x, 1)",
            read={
                "starrocks": "SELECT PERCENTILE_APPROX_RAW(x,1)",
            },
        )
        self.validate_all(
            "SELECT MAX_BY(a, b), MIN_BY(c, d)",
            read={"clickhouse": "SELECT argMax(a, b), argMin(c, d)"},
        )
        self.validate_all(
            "SELECT ARRAY_JOIN(ARRAY('a', 'b', 'c', NULL),'#')",
            read={"postgres": "SELECT array_to_string(['a','b','c',null],'#')"},
        )
        self.validate_all(
            "SELECT ARRAY_JOIN(ARRAY('a', 'b', 'c', NULL),'#','*')",
            read={"postgres": "SELECT array_to_string(['a','b','c',null],'#','*')"},
        )
        self.validate_all(
            "SELECT CONCAT_WS('',ARRAY('12/05/2021', '12:50:00')) AS DateString",
            read={
                "clickhouse": "SELECT arrayStringConcat(['12/05/2021', '12:50:00']) AS DateString"
            },
        )
        self.validate_all(
            "SELECT CONCAT_WS('*', ARRAY('12/05/2021', '12:50:00')) AS DateString",
            read={
                "clickhouse": "SELECT arrayStringConcat(['12/05/2021', '12:50:00'], '*') AS DateString"
            },
        )
        self.validate_all(
            "${a}",
            read={"presto": "${a}"},
        )

        self.validate_all(
            "SELECT aa, sum(CASE WHEN index_name = 'ceshi' THEN score ELSE 0 END) AS avg_score FROM table GROUP BY aa",
            read={
                "presto": "select aa,sum(score) filter(where index_name='ceshi') as avg_score from table group by aa"
            },
        )

        self.validate_all(
            "SELECT CAST('2024-01-16' AS STRING)",
            read={"clickhouse": "SELECT TOSTRING('2024-01-16')"},
        )
        self.validate_all(
            "SELECT HOURS_DIFF(CAST('2018-01-02 23:00:00' AS DATETIME), CAST('2018-01-01 22:00:00' AS DATETIME))",
            read={
                "clickhouse": "SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));"
            },
        )
        self.validate_all(
            "SELECT ARRAY_AVG(ARRAY(1, 2, 4))",
            read={"clickhouse": "SELECT arrayAvg([1, 2, 4]);"},
        )
        self.validate_all(
            "SELECT ARRAY_DISTINCT(ARRAY(1, 1, 2, 3, 3, 3))",
            read={"clickhouse": "SELECT arrayCompact([1, 1, 2, 3, 3, 3])"},
        )
        self.validate_all(
            "SELECT ARRAY_CUM_SUM(ARRAY(1, 1, 1, 1))",
            read={"clickhouse": "SELECT ARRAY_CUM_SUM(ARRAY(1, 1, 1, 1))"},
        )
        self.validate_all(
            "SELECT ARRAY_DIFFERENCE(ARRAY(1, 2, 3, 4))",
            read={"clickhouse": "SELECT arrayDifference([1, 2, 3, 4])"},
        )
        self.validate_all(
            "SELECT ARRAY_DISTINCT(ARRAY(1, 2, 2, 3, 1))",
            read={"clickhouse": "SELECT arrayDistinct([1, 2, 2, 3, 1])"},
        )
        self.validate_all(
            "SELECT ARRAY_EXISTS(x -> x > 1, ARRAY(1, 2, 3))",
            read={"clickhouse": "SELECT arrayexists(x->x>1,[1,2,3])"},
        )
        self.validate_all(
            "SELECT ARRAY_FILTER(x -> x LIKE '%World%',ARRAY('Hello', 'abc World')) AS res",
            read={
                "clickhouse": "SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res"
            },
        )
        self.validate_all(
            "SELECT ARRAY_FIRST(x -> x > 2, ARRAY(1, 2, 3, 0))",
            read={"clickhouse": "select arrayfirst(x->x>2, [1,2,3,0])"},
        )
        self.validate_all(
            "SELECT ARRAY_FIRST_INDEX(x -> x + 1 > 3, ARRAY(2, 3, 4))",
            read={"clickhouse": "select arrayFirstIndex(x->x+1>3, [2, 3, 4])"},
        )
        self.validate_all(
            "SELECT ARRAY_INTERSECT(ARRAY(1, 2), ARRAY(1, 3))",
            read={"clickhouse": "SELECT arrayIntersect([1, 2], [1, 3])"},
        )
        self.validate_all(
            "SELECT ARRAY_LAST(x -> x > 2, ARRAY(1, 2, 3, 0))",
            read={"clickhouse": "select arrayLast(x->x>2, [1,2,3,0])"},
        )
        self.validate_all(
            "SELECT ARRAY_LAST_INDEX(x -> x + 1 > 3, ARRAY(2, 3, 4))",
            read={"clickhouse": "select arrayLastIndex(x->x+1>3, [2, 3, 4])"},
        )
        self.validate_all(
            "SELECT ARRAY_MAP(x -> (x + 2), ARRAY(1, 2, 3)) AS res",
            read={"clickhouse": "SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res"},
        )
        self.validate_all(
            "SELECT ARRAY_PRODUCT(ARRAY(1, 2, 3, 4, 5, 6))",
            read={"clickhouse": "SELECT arrayProduct([1,2,3,4,5,6])"},
        )
        self.validate_all(
            "SELECT ARRAY_REVERSE_SORT(ARRAY('hello', 'world', '!'))",
            read={"clickhouse": "SELECT arrayReverseSort(['hello', 'world', '!'])"},
        )
        self.validate_all(
            "SELECT ARRAY_SUM(x -> x * x, ARRAY(2, 3))",
            read={"clickhouse": "SELECT arraySum(x -> x*x, [2, 3])"},
        )
        self.validate_all(
            "SELECT SIZE(ARRAY_DISTINCT(ARRAY(1, 1, 2, 3, 3, 3)))",
            read={"clickhouse": "SELECT arrayUniq([1, 1, 2, 3, 3, 3])"},
        )
        self.validate_all(
            "SELECT ARRAY_ZIP(ARRAY('a', 'b', 'c'), ARRAY(5, 2, 1))",
            read={"clickhouse": "SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1])"},
        )
        self.validate_all(
            "${a}",
            read={"presto": "${a}"},
        )

        self.validate_all(
            "SELECT aa, sum(CASE WHEN index_name = 'ceshi' THEN score ELSE 0 END) AS avg_score FROM table GROUP BY aa",
            read={
                "presto": "select aa,sum(score) filter(where index_name='ceshi') as avg_score from table group by aa"
            },
        )

        self.validate_all(
            "REPLACE('www.baidu.com:9090','9090','')",
            read={
                # "clickhouse": "REPLACEALL('www.baidu.com:9090','9090','')",
                "presto": "REPLACE('www.baidu.com:9090','9090')",
            },
        )
        self.validate_all(
            "SELECT TO_DATE('2022-12-30 01:02:03')",
            read={"clickhouse": "SELECT toDate('2022-12-30 01:02:03')"},
        )
        self.validate_all(
            "SELECT YEAR(a), QUARTER(a), MONTH(a), HOUR(a), MINUTE(a), SECOND(a), UNIX_TIMESTAMP(a)",
            read={
                "clickhouse": "SELECT toYear(a), toQuarter(a),toMonth(a), toHour(a), toMinute(a), toSecond(a), toUnixTimestamp(a)"
            },
        )
        self.validate_all(
            "SELECT YEARS_ADD(x, 1), MONTHS_ADD(x, 1), WEEKS_ADD(x, 1), DAYS_ADD(x, 1), HOURS_ADD(x, 1), SECONDS_ADD(x, 1), MONTHS_ADD(x,3)",
            read={
                "clickhouse": "SELECT  addYears(x, 1), addMonths(x, 1), addWeeks(x, 1), addDays(x, 1), addHours(x, 1), addSeconds(x, 1), addQuarters(x, 1)"
            },
        )
        self.validate_all(
            "SELECT YEARS_SUB(x, 1), MONTHS_SUB(x, 1), MONTHS_SUB(x, 1), MONTHS_SUB(x,3)",
            read={
                "clickhouse": "SELECT  subtractYears(x, 1), subtractMonths(x, 1), subtractSeconds(x, 1), subtractQuarters(x, 1)"
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT(x, '%Y%m'), DATE_FORMAT(x, '%Y%m%d'), DATE_FORMAT(x, '%Y%m%d%H%i%s'), DATE_TRUNC(x, 'Quarter'), DATE_TRUNC(x, 'Quarter'), DATE_TRUNC(x, 'Quarter'), DATE_TRUNC(x, 'Quarter'), DATE_TRUNC(x, 'Quarter'), DATE_TRUNC(x, 'Quarter'), DATE_TRUNC(x, 'Quarter')",
            read={
                "clickhouse": "SELECT toYYYYMM(x, 'US/Eastern'), toYYYYMMDD(x, 'US/Eastern'), toYYYYMMDDHHMMSS(x, 'US/Eastern'), toStartOfQuarter(x),  toStartOfMonth(x), toStartOfWeek(x), toStartOfDay(x), toStartOfHour(x), toStartOfMinute(x), toStartOfSecond(x)"
            },
        )

        self.validate_all(
            "SHA2(x,256)",
            read={
                "presto": "SHA256(x)",
            },
        )

    def test_identity(self):
        self.validate_identity("COALECSE(a, b, c, d)")
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")
        self.validate_identity("ARRAY_SORT(x)", "ARRAY_SORT(x)")
        self.validate_identity("DATE_ADD(x,1)", "DATE_ADD(x, INTERVAL 1 DAY)")
        self.validate_identity("DATE_SUB(x,1)", "DATE_SUB(x, INTERVAL 1 DAY)")
        self.validate_identity("DATEDIFF(x,1)", "DATEDIFF(x, 1)")
        self.validate_identity("GROUP_ARRAY(x)", "COLLECT_LIST(x)")
        self.validate_identity("NOW()", "NOW()")
        self.validate_identity("SIZE(x)", "ARRAY_SIZE(x)")
        self.validate_identity("SPLIT_BY_STRING(x,',')", "SPLIT_BY_STRING(x, ',')")
        self.validate_identity("VAR_SAMP(x)", "STDDEV_SAMP(x)")
        self.validate_identity("3&5", "BITAND(3, 5)")
        self.validate_identity("3|5", "BITOR(3, 5)")
        self.validate_identity("3^5", "BITXOR(3, 5)")
        self.validate_identity("~5", "BITNOT(5)")
        self.validate_identity("random(2)", "FLOOR(RANDOM()*2.0)")
        self.validate_identity("random(2,3)", "FLOOR(RANDOM()*1.0+2.0)")

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")

        self.validate_all(
            "WEEK(CAST('2010-01-01' AS DATE), 3)",
            read={
                "presto": "week(DATE '2010-01-01')",
            },
        )

    def test_regex(self):
        self.validate_all(
            "SELECT REGEXP_LIKE(abc, '%foo%')",
            write={
                "doris": "SELECT REGEXP(abc, '%foo%')",
            },
        )

        self.validate_all(
            "SELECT REGEXP_EXTRACT('Abcd abCd aBcd', '(ab.)', 1)",
            read={
                "postgres": "SELECT regexp_match('Abcd abCd aBcd', 'ab.')",
            },
        )

        self.validate_all(
            "SELECT REGEXP_EXTRACT_ALL('abcd abcd abcd', '(ab.)')",
            read={
                "postgres": "SELECT regexp_matches('abcd abcd abcd', 'ab.')",
            },
        )

    def test_array(self):
        self.validate_all(
            "SELECT SIZE(ARRAY_DISTINCT(x))",
            read={"clickhouse": "SELECT ARRAYUNIQ(x)"},
        )
        self.validate_all(
            "ARRAY_SORT(x)",
            read={
                "clickhouse": "ARRAYSORT(x)",
            },
        )
        self.validate_all(
            "ARRAY_MAP(x -> x + 1, ARRAY(5, 6))",
            read={"presto": "transform(ARRAY [5, 6], x -> x + 1)"},
        )
        self.validate_all(
            "SELECT ARRAY_POPBACK(ARRAY(1, 2, 3)), ARRAY_POPFRONT(ARRAY(1, 2, 3)), ARRAY_PUSHBACK(ARRAY(1, 2, 3)), ARRAY_PUSHFRONT(ARRAY(1, 2, 3))",
            read={
                "clickhouse": "select arrayPopBack([1, 2, 3]), arrayPopFront([1, 2, 3]),  arrayPushBack([1, 2, 3]), arrayPushFront([1, 2, 3])"
            },
        )
        self.validate_all(
            "ARRAY_SLICE(ARRAY(1, 2, NULL, 4, 5), 2, 3)",
            read={
                "clickhouse": "arraySlice([1, 2, NULL, 4, 5], 2, 3) ",
            },
        )

    def test_bit(self):
        self.validate_all(
            "GROUP_BIT_AND(x)",
            read={
                "postgres": "BIT_AND(x)",
                "clickhouse": "GROUPBITAND(x)",
                "snowflake": "BITAND_AGG(x)",
            },
        )
        self.validate_all(
            "GROUP_BIT_OR(x)",
            read={
                "postgres": "BIT_OR(x)",
                "clickhouse": "GROUPBITOR(x)",
                "snowflake": "BITOR_AGG(x)",
            },
        )
        self.validate_all(
            "GROUP_BIT_XOR(x)",
            read={
                "postgres": "BIT_XOR(x)",
                "clickhouse": "GROUPBITXOR(x)",
                "snowflake": "BITXOR_AGG(x)",
            },
        )

    def test_varchar(self):
        self.validate_all(
            "LOCATE('a', 'abc')",
            read={
                "presto": "index('abc','a')",
            },
        )
