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
                "clickhouse": "replaceAll('www.baidu.com:9090','9090','')",
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
            "SELECT YEARS_SUB(x, 1), MONTHS_SUB(x, 1), SECONDS_SUB(x, 1), MONTHS_SUB(x,3)",
            read={
                "clickhouse": "SELECT  subtractYears(x, 1), subtractMonths(x, 1), subtractSeconds(x, 1), subtractQuarters(x, 1)"
            },
        )
        self.validate_all(
            "SELECT DATE_FORMAT(x, '%Y%m'), DATE_FORMAT(x, '%Y%m%d'), DATE_FORMAT(x, '%Y%m%d%H%i%s'), DATE_TRUNC(x, 'Quarter'), DATE_TRUNC(x, 'Month'), DATE_TRUNC(x, 'Week'), DATE_TRUNC(x, 'Day'), DATE_TRUNC(x, 'Hour'), DATE_TRUNC(x, 'Minute'), DATE_TRUNC(x, 'Second')",
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
        self.validate_all(
            "NULL_OR_EMPTY('')",
            read={
                "clickhouse": "empty('')",
            },
        )
        self.validate_all(
            "NOT_NULL_OR_EMPTY('')",
            read={
                "clickhouse": "NotEmpty('')",
            },
        )
        self.validate_all(
            "CHAR_LENGTH('x')",
            read={
                "clickhouse": "lengthUTF8('x')",
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
        self.validate_identity("3&5", "BITAND(3, 5)")
        self.validate_identity("3|5", "BITOR(3, 5)")
        self.validate_identity("3^5", "BITXOR(3, 5)")
        self.validate_identity("~5", "BITNOT(5)")
        self.validate_identity("random(2)", "FLOOR(RANDOM()*2.0)")
        self.validate_identity("random(2,3)", "FLOOR(RANDOM()*1.0+2.0)")
        self.validate_identity("a||b", "CONCAT(a,b)")
        self.validate_identity(
            "select * from t where comment Match_Any 'OLAP'",
            "SELECT * FROM t WHERE comment MATCH_ANY 'OLAP'",
        )
        self.validate_identity(
            "select * from t where comment Match_All 'OLAP'",
            "SELECT * FROM t WHERE comment MATCH_ALL 'OLAP'",
        )
        self.validate_identity(
            "select * from t where comment MATCH_PHRASE 'OLAP'",
            "SELECT * FROM t WHERE comment MATCH_PHRASE 'OLAP'",
        )

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")
        self.validate_all(
            "WEEK(CAST('2010-01-01' AS DATE), 3)",
            read={
                "presto": "week(DATE '2010-01-01')",
            },
        )
        self.validate_all(
            "DATE_FORMAT(FROM_UNIXTIME(1609167953694 / 1000), '%Y-%m-%d')",
            read={
                "presto": "format_datetime(from_unixtime(1609167953694/1000),'yyyy-MM-dd')",
            },
        )
        self.validate_all(
            "DATE_SUB(TO_DATE('2018-01-01'), INTERVAL 3 YEAR)",
            read={
                "clickhouse": "date_sub(YEAR, 3, toDate('2018-01-01'))",
            },
        )
        self.validate_all(
            "DATE_TRUNC(NOW(), 'day')",
            read={
                "hive": "TRUNC(current_timestamp(), 'DD')",
                "oracle": "TRUNC(current_timestamp(), 'DD')",
            },
        )
        self.validate_all(
            "NOW()",
            read={
                "hive": "SYSDATE",
                "oracle": "SYSDATE",
                "redshift": "SYSDATE",
            },
        )
        self.validate_all(
            "DATE_FORMAT(CAST('2022-08-20 08:23:42' AS DATETIME), '%Y-%m-%d %H:%i:%s')",
            read={
                "presto": "format_datetime(TIMESTAMP '2022-08-20 08:23:42', 'yyyy-MM-dd HH:mm:ss')"
            },
        )
        self.validate_all(
            "DATE_FORMAT(x, '%Y')",
            read={"presto": "to_date(x,'yyyy') "},
        )
        self.validate_all(
            "DATE_FORMAT(x, '%Y-%m-%d %H:%i:%s')",
            read={"presto": "to_date(x,'yyyy-MM-dd hh24:mi:ss')"},
        )
        # self.validate_all(
        #     "STR_TO_DATE('2005-01-01 13:14:20', '%Y-%m-%d %H:%i:%s')",
        #     read={
        #         "oracle": "to_date('2005-01-01 13:14:20','yyyy-MM-dd hh24:mm:ss')"
        #     }
        # )

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
                "clickhouse": "SELECT extractAll('abcd abcd abcd', 'ab.')",
            },
        )
        self.validate_all(
            "REGEXP_REPLACE_ONE('Hello, World!', '.*', '*****')",
            read={
                "clickhouse": "replaceRegexpOne('Hello, World!', '.*', '*****')",
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
            "SELECT ARRAY_POPBACK(ARRAY(1, 2, 3))",
            read={
                "clickhouse": "select arrayPopBack([1, 2, 3])",
            },
        )
        self.validate_all(
            "SELECT ARRAY_POPFRONT(ARRAY(1, 2, 3))",
            read={
                "clickhouse": "select  arrayPopFront([1, 2, 3])",
            },
        )
        self.validate_all(
            "SELECT ARRAY_PUSHBACK(ARRAY(1, 2, 3), 4)",
            read={
                "clickhouse": "select arrayPushBack([1, 2, 3], 4)",
                "postgres": "select ARRAY_APPEND([1, 2, 3], 4)",
            },
        )
        self.validate_all(
            "SELECT ARRAY_PUSHFRONT(ARRAY(1, 2, 3), 4)",
            read={
                "clickhouse": "select arrayPushFront([1, 2, 3], 4)",
                "postgres": "select ARRAY_PREPEND(4, [1, 2, 3])",
            },
        )
        self.validate_all(
            "ARRAY_SLICE(ARRAY(1, 2, NULL, 4, 5), 2, 3)",
            read={
                "clickhouse": "arraySlice([1, 2, NULL, 4, 5], 2, 3) ",
            },
        )
        self.validate_all(
            "ARRAY_CONTAINS(ARRAY(1, 2, NULL), NULL)",
            read={
                "clickhouse": "has([1, 2, NULL], NULL) ",
            },
        )
        self.validate_all(
            "ARRAY_RANGE(0, 5)",
            read={
                "clickhouse": "range(0, 5) ",
                "presto": "sequence(0, 5)",
            },
        )
        self.validate_all(
            "ARRAY_SHUFFLE(x)",
            read={
                "presto": "Shuffle(x)",
            },
        )
        self.validate_all(
            "ARRAY_SLICE(ARRAY(1, 2, 3), 1)",
            read={
                "presto": "slice([1,2,3],1)",
            },
        )
        self.validate_all(
            "ELEMENT_AT(ARRAY(1, 2, 3), 1)",
            read={
                "clickhouse": "arrayElement([1, 2, 3],1)",
            },
        )
        self.validate_all("arr_int[1]", read={"presto": "element_at(arr_int, 1)"})
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
            read={"clickhouse": "SELECT arrayCumSum(ARRAY(1, 1, 1, 1))"},
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
            read={
                "clickhouse": "SELECT arraySum(x -> x*x, [2, 3])",
            },
            write={
                "clickhouse": "SELECT arraySum(x -> x * x, [2, 3])",
                "doris": "SELECT ARRAY_SUM(x -> x * x, ARRAY(2, 3))",
            },
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
            "SELECT ARRAY_ZIP(ARRAY('a', 'b', 'c'), ARRAY(5, 2, 1))",
            read={"clickhouse": "SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1])"},
        )
        self.validate_all(
            "COLLECT_LIST(res)",
            read={"clickhouse": " groupArray(res)"},
        )
        self.validate_all(
            "LAST_DAY(x)",
            read={
                "clickhouse": " toLastDayOfMonth(x)",
                "presto": " last_day_of_month(x)",
            },
        )

    def test_bitmap(self):
        self.validate_all(
            "BITMAP_FROM_ARRAY(ARRAY(1, 2, 3, 4, 5))",
            read={
                "clickhouse": "bitmapBuild([1, 2, 3, 4, 5]) ",
            },
        )
        self.validate_all(
            "BITMAP_TO_ARRAY(BITMAP_FROM_ARRAY(ARRAY(1, 2, 3, 4, 5)))",
            read={
                "clickhouse": "bitmapToArray(bitmapBuild([1, 2, 3, 4, 5])) ",
            },
        )
        self.validate_all(
            "BITMAP_AND(a, b)",
            read={
                "clickhouse": "bitmapAnd(a,b)",
            },
        )
        self.validate_all(
            "BITMAP_AND_COUNT(a, b)",
            read={
                "clickhouse": "bitmapAndCardinality(a,b)",
            },
        )
        self.validate_all(
            "BITMAP_AND_NOT(a, b)",
            read={
                "clickhouse": "bitmapAndnot(a,b)",
            },
        )
        self.validate_all(
            "BITMAP_AND_NOT_COUNT(a, b)",
            read={
                "clickhouse": "bitmapAndnotCardinality(a,b)",
            },
        )
        self.validate_all(
            "BITMAP_COUNT(BITMAP_FROM_ARRAY(ARRAY(1, 2, 3, 4, 5)))",
            read={
                "clickhouse": "bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5]))",
            },
        )
        self.validate_all(
            "BITMAP_CONTAINS(BITMAP_FROM_ARRAY(ARRAY(1, 5, 7, 9)), 9)",
            read={
                "clickhouse": "bitmapContains(bitmapBuild([1,5,7,9]), 9)",
            },
        )
        self.validate_all(
            "BITMAP_OR_COUNT(a, b)",
            read={
                "clickhouse": "bitmapOrCardinality(a, b)",
            },
        )
        self.validate_all(
            "BITMAP_XOR_COUNT(a, b)",
            read={
                "clickhouse": "bitmapXorCardinality(a, b)",
            },
        )
        self.validate_all(
            "BITMAP_HAS_ANY(a, b)",
            read={
                "clickhouse": "bitmapHasAny(a, b)",
            },
        )
        self.validate_all(
            "BITMAP_HAS_ALL(a, b)",
            read={
                "clickhouse": "bitmapHasAll(a, b)",
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

    def test_ip(self):
        self.validate_all(
            "IPV4_STRING_TO_NUM_OR_DEFAULT(addr)",
            read={
                "clickhouse": "IPv4StringToNumOrDefault(addr)",
            },
        )
        self.validate_all(
            "IPV6_STRING_TO_NUM_OR_DEFAULT(addr)",
            read={
                "clickhouse": "IPv6StringToNumOrDefault(addr)",
            },
        )
        self.validate_all(
            "IS_IPV4_STRING(addr)",
            read={
                "clickhouse": "IsIPv4String(addr)",
            },
        )
        self.validate_all(
            "IS_IPV6_STRING(addr)",
            read={
                "clickhouse": "IsIPv6String(addr)",
            },
        )

    def test_varchar(self):
        self.validate_all(
            "LOCATE('a', 'abc')",
            read={
                "presto": "index('abc','a')",
                "clickhouse": "position('abc','a')",
                "postgres": "strpos('abc','a')",
            },
        )
        self.validate_all(
            "LOWER('ABcdEf')",
            read={
                "clickhouse": "lowerUTF8('ABcdEf')",
            },
        )
        self.validate_all(
            "UPPER('ABcdEf')",
            read={
                "clickhouse": "upperUTF8('ABcdEf')",
            },
        )
        self.validate_all(
            "SUBSTRING('ABcdEf', 1, 2)",
            read={
                "clickhouse": "substringUTF8('ABcdEf',1,2)",
            },
        )
        self.validate_all(
            "ENDS_WITH('中国', '国')",
            read={
                "clickhouse": "endsWith('中国', '国')",
            },
        )
        self.validate_all(
            "STARTS_WITH('hello doris', 'hello')",
            read={
                "clickhouse": "startsWith('hello doris', 'hello')",
            },
        )
        self.validate_all(
            "LTRIM('     Hello, world!     ')",
            read={
                "clickhouse": "trimLeft('     Hello, world!     ')",
            },
        )
        self.validate_all(
            "RTRIM('     Hello, world!     ')",
            read={
                "clickhouse": "trimRIGHT('     Hello, world!     ')",
            },
        )
        self.validate_all(
            "SPLIT_BY_STRING('adidas', 'a')",
            read={
                "clickhouse": "splitByString('a', 'adidas')",
            },
        )
        self.validate_all(
            "MULTI_MATCH_ANY('Hello, World!', ARRAY('hello', '!', 'world'))",
            read={
                "clickhouse": "multiMatchAny('Hello, World!', ['hello', '!', 'world'])",
            },
        )
        self.validate_all(
            "LENGTH('x')",
            read={
                "postgres": "octet_length('x')",
                "oracle": "lengthb('x')",
            },
        )
        self.validate_all(
            "CHAR_LENGTH('x')",
            read={
                "clickhouse": "lengthUTF8('x')",
                "oracle": "length('x')",
            },
        )
        self.validate_all(
            "CONCAT_WS(',', 'abcde', 2, NULL, 22)",
            read={
                "postgres": "concat_ws(',', 'abcde', 2, NULL, 22);",
            },
        )
        self.validate_all(
            "DATE_FORMAT(NOW(), '%d')",
            read={
                "oracle": "to_char(sysdate,'dd')",
            },
        )
        self.validate_all(
            "ROUND(1210.73, 2)",
            read={
                "oracle": "to_char(1210.73, '9999.99')",
            },
        )
        self.validate_all(
            "DATE_FORMAT(CURRENT_DATE(), '%Y-%m')",
            read={"presto": "to_char(current_date, 'yyyy-mm')"},
        )
        self.validate_all(
            "DATE_FORMAT(DATE_ADD(CAST(day AS DATE), INTERVAL 1 DAY), '%d') = '01'",
            read={"presto": "to_char(date_add('day', 1, cast(day as date)),'dd') ='01'"},
        )

    def test_code(self):
        self.validate_all(
            "TO_BASE64('x')",
            read={
                "clickhouse": "base64Encode('x')",
            },
        )
        self.validate_all(
            "FROM_BASE64('x')",
            read={
                "clickhouse": "base64Decode('x')",
            },
        )
        self.validate_all(
            "HEX('x')",
            read={
                "postgres": "to_hex('x')",
            },
        )

    def test_geography(self):
        self.validate_all(
            "ST_ASTEXT(ST_POINT(-122.35, 37.55))",
            read={
                "snowflake": "ST_GEOGRAPHYFROMWKB('POINT(-122.35 37.55)')",
            },
        )

    def test_json(self):
        self.validate_all(
            "JSONB_EXTRACT('{\"id\": \"33\"}', '$.id')",
            read={
                "clickhouse": "JSONExtractString('{\"id\": \"33\"}' , 'id')",
            },
        )
        self.validate_all(
            "JSONB_EXTRACT('{\"id\": \"33\"}', '$.id')",
            read={
                "clickhouse": "JSONExtractRaw('{\"id\": \"33\"}' , 'id')",
            },
        )
        self.validate_all(
            "JSONB_EXTRACT('{\"id\": \"33\"}', '$.id')",
            read={
                "clickhouse": "JSONExtractInt('{\"id\": \"33\"}' , 'id')",
            },
        )
        self.validate_all(
            "JSONB_EXTRACT('{\"id\": \"33\"}', '$.id.name')",
            read={
                "clickhouse": "JSONExtractString('{\"id\": \"33\"}' , 'id', 'name')",
            },
        )
        self.validate_all(
            'JSONB_EXTRACT(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'$.f4\')',
            read={
                "postgres": 'json_extract_path(\'{"f2":{"f3":1},"f4":{"f5":99,"f6":"foo"}}\', \'f4\')',
            },
        )
        self.validate_all(
            "JSON_CONTAINS(x, '1')",
            read={
                "mysql": "JSON_ARRAY_CONTAINS(x, '1')",
            },
        )
        self.validate_all(
            "JSON_PARSE(x)",
            read={
                "presto": "JSON_PARSE(x)",
                "snowflake": "PARSE_JSON(x)",
                "bigquery": "PARSE_JSON(x)",
            },
        )
        self.validate_all(
            "JSON_LENGTH(x)",
            read={
                "presto": "JSON_ARRAY_LENGTH(x)",
                "trino": "JSON_ARRAY_LENGTH(x)",
            },
        )

    def test_math(self):
        self.validate_all(
            "STDDEV_POP(x)",
            read={
                "clickhouse": "stddevPop(x)",
            },
        )
        self.validate_all(
            "STDDEV_SAMP(x)",
            read={
                "clickhouse": "stddevSamp(x)",
            },
        )
        self.validate_all(
            "VAR_SAMP(x)",
            read={
                "clickhouse": "varSamp(x)",
            },
        )
        self.validate_all(
            "VARIANCE_POP(x)",
            read={
                "clickhouse": "varPop(x)",
            },
        )
        self.validate_all(
            "POWER(2, 3)",
            read={
                "clickhouse": "exp2(3)",
            },
        )
        self.validate_all(
            "POWER(10, 3)",
            read={
                "clickhouse": "EXP10(3)",
            },
        )
        self.validate_all(
            "LOG10(x)",
            read={
                "duckdb": "LOG(x)",
                "postgres": "LOG(x)",
                "redshift": "LOG(x)",
                "sqlite": "LOG(x)",
                "teradata": "LOG(x)",
            },
        )
        self.validate_all(
            "SELECT TRUNCATE(123.458, 1)",
            read={"hive": "select trunc(123.458,1)", "oracle": "select trunc(123.458,1)"},
        )
        self.validate_all(
            "SELECT TRUNCATE(123.458, -1)",
            read={"hive": "select trunc(123.458,-1)", "oracle": "select trunc(123.458,-1)"},
        )
        self.validate_all(
            "TRUNCATE(123)",
            read={
                "postgres": "trunc(123)",
                "hive": "trunc(123)",
                "oracle": "trunc(123)",
            },
        )

    def test_Quoting(self):
        self.validate_all(
            "SELECT `a` FROM t1",
            read={
                "presto": 'select "a" from t1',
            },
        )

    def test_agg(self):
        self.validate_all(
            "GROUP_CONCAT(`base_caseNumber_labelObject`, '|')",
            read={"doris": "GROUP_CONCAT(`base_caseNumber_labelObject`, '|')"},
            write={
                "redshift": "LISTAGG(\"base_caseNumber_labelObject\", '|')",
                "snowflake": "LISTAGG(\"base_caseNumber_labelObject\", '|')",
                "postgres": "STRING_AGG(\"base_caseNumber_labelObject\", '|')",
            },
        )

    def test_explain(self):
        self.validate_all(
            "explain SELECT * FROM (SELECT id, sum(CASE WHEN a = '2' THEN cost ELSE 0 END) AS avg FROM t GROUP BY id)",
            read={
                "presto": "explain select * from (select id,sum(cost) filter(where a='2') as avg from t group by id)",
            },
        )
        self.validate_all(
            "EXPLAIN SHAPE PLAN SELECT COUNT(*), ANY_VALUE(y) FROM (SELECT COUNT(*) FROM test1)",
            read={
                "presto": "explain shape plan select count(*),arbitrary(y) from (select count(*) from test1)",
            },
        )
        self.validate_all(
            "EXPLAIN VERBOSE SELECT COUNT(*), ANY_VALUE(y) FROM (SELECT COUNT(*) FROM test1)",
            read={
                "presto": "explain verbose select count(*),arbitrary(y) from (select count(*) from test1)",
            },
        )
        self.validate_all(
            "EXPLAIN MEMO PLAN SELECT COUNT(*), ANY_VALUE(y) FROM (SELECT COUNT(*) FROM test1)",
            read={
                "presto": "explain memo plan select count(*),arbitrary(y) from (select count(*) from test1)",
            },
        )
        self.validate_all(
            "EXPLAIN PHYSICAL PLAN SELECT COUNT(*), ANY_VALUE(y) FROM (SELECT COUNT(*) FROM test1)",
            read={
                "presto": "explain physical plan select count(*),arbitrary(y) from (select count(*) from test1)",
            },
        )

    def test_case_sensitive(self):
        import sqlglot
        from sqlglot.optimizer.qualify_tables import qualify_tables

        expected_result_1 = "SELECT * FROM T AS T"
        input_sql_1 = """select * from t"""
        result_1 = qualify_tables(
            sqlglot.parse_one(read="presto", sql=input_sql_1), case_sensitive=True
        ).sql("doris")
        assert (
            result_1 == expected_result_1
        ), f"Transpile result doesn't match expected result. Expected: {expected_result_1}, Actual: {result_1}"
        print("Test1 passed!")

        expected_result_2 = "SELECT * FROM t AS t"
        input_sql_2 = """select * from T"""
        result_2 = qualify_tables(
            sqlglot.parse_one(read="presto", sql=input_sql_2), case_sensitive=False
        ).sql("doris")
        assert (
            result_2 == expected_result_2
        ), f"Transpile result doesn't match expected result. Expected: {expected_result_2}, Actual: {result_2}"
        print("Test2 passed!")
