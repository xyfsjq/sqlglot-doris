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

    def test_identity(self):
        self.validate_identity("COALECSE(a, b, c, d)")
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")
        self.validate_identity("ARRAY_SORT(x)", "SORT_ARRAY(x)")
        self.validate_identity("COUNTEQUAL(x,2)", "REPEAT(x, 2)")
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

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")

    def test_regex(self):
        self.validate_all(
            "SELECT REGEXP_LIKE(abc, '%foo%')",
            write={
                "doris": "SELECT REGEXP(abc, '%foo%')",
            },
        )

    def test_array(self):
        self.validate_all(
            "SELECT SIZE(ARRAY_DISTINCT(x))",
            read={"clickhouse": "SELECT ARRAYUNIQ(x)"},
        )
