import sqlglot
from tests.dialects.test_dialect import Validator


class TestDoris(Validator):
    dialect = "doris"

    def test_identity(self):
        self.validate_identity("COALECSE(a, b, c, d)")
        self.validate_identity("SELECT CAST(`a`.`b` AS INT) FROM foo")
        self.validate_identity("SELECT APPROX_COUNT_DISTINCT(a) FROM x")

    def test_time(self):
        self.validate_identity("TIMESTAMP('2022-01-01')")

    def test_regex(self):
        self.validate_all(
            "SELECT REGEXP_LIKE(abc, '%foo%')",
            write={
                "doris": "SELECT REGEXP(abc, '%foo%')",
            },
        )

    def test_date_format(self):
        expected_result_1 = "DATE_FORMAT(NOW(), '%Y')"
        input_sql_1 = """to_char(sysdate, 'yyyy')"""
        result_1 = sqlglot.transpile(input_sql_1, read="oracle", write="doris", pretty=True)[0]
        assert (
            result_1 == expected_result_1
        ), f"Transpile result doesn't match expected result. Expected: {expected_result_1}, Actual: {result_1}"
        print("Test1 passed!")

        expected_result_2 = "DATE_TRUNC(DATE(NOW()), 'year')"
        input_sql_2 = """trunc(trunc(sysdate), 'YYYY')"""
        result_2 = sqlglot.transpile(input_sql_2, read="oracle", write="doris", pretty=True)[0]
        assert (
            result_2 == expected_result_2
        ), f"Transpile result doesn't match expected result. Expected: {expected_result_2}, Actual: {result_2}"
        print("Test2 passed!")

        expected_result_3 = "DATE(NOW())"
        input_sql_3 = """trunc(sysdate)"""
        result_3 = sqlglot.transpile(input_sql_3, read="oracle", write="doris", pretty=True)[0]
        assert (
            result_3 == expected_result_3
        ), f"Transpile result doesn't match expected result. Expected: {expected_result_3}, Actual: {result_3}"
        print("Test3 passed!")

        expected_result_4 = "DATE_TRUNC(NOW(), 'day')"
        input_sql_4 = """TRUNC(current_timestamp(), 'DD')"""
        result_4 = sqlglot.transpile(input_sql_4, read="hive", write="doris", pretty=True)[0]
        assert (
            result_4 == expected_result_4
        ), f"Transpile result doesn't match expected result. Expected: {expected_result_4}, Actual: {result_4}"
        print("Test4 passed!")

        expected_result_5 = """SELECT
  REPLACE(SUBSTRING(DATE_FORMAT(t1.dateOfDay, '%Y-%m-%d'), 6, 5),'-','/') AS dateOfDay,
  COALESCE(t2.paidPeopleNum, 0) AS paidPeopleNum
FROM (
  SELECT
    CURRENT_DATE() + GENERATE_SERIES(-7, -1) AS dateOfDay
) AS t1
LEFT JOIN (
  SELECT
    DATE(o.pay_time) AS dateOfDay,
    COUNT(DISTINCT o.user_id) AS paidPeopleNum
  FROM tbl_order AS o
  WHERE
    o.pay_time >= '2020-04-29 00:00:00' AND o.order_status = 3
  GROUP BY
    DATE(o.pay_time)
) AS t2
  ON t2.dateOfDay = t1.dateOfDay
ORDER BY
  t1.dateOfDay"""
        input_sql_5 = """SELECT 
              REPLACE(
                SUBSTRING(to_char(t1.dateOfDay, 'YYYY-MM-DD') FROM 6 FOR 5 ), 
              '-', '/' ) AS dateOfDay,
              COALESCE(t2.paidPeopleNum, 0) AS paidPeopleNum 
            FROM( 
              SELECT CURRENT_DATE + generate_series(-7, -1) AS dateOfDay
            ) t1
            LEFT JOIN(
              SELECT 
                DATE(o.pay_time) AS dateOfDay,
                COUNT(DISTINCT o.user_id) AS paidPeopleNum 
              FROM tbl_order o
              WHERE o.pay_time >= '2020-04-29 00:00:00' AND o.order_status = 3 
              GROUP BY DATE(o.pay_time) 
            ) t2 ON t2.dateOfDay = t1.dateOfDay 
            ORDER BY t1.dateOfDay;
            """
        result_5 = sqlglot.transpile(input_sql_5, read="postgres", write="doris", pretty=True)[0]
        assert (
            result_5 == expected_result_5
        ), f"Transpile result doesn't match expected result. Expected: {expected_result_5}, Actual: {result_5}"
        print("Test5 passed!")

    def test_json(self):
        expected_result_6 = "SELECT JSON_CONTAINS('[1, 2, 3]','2')"
        input_sql_6 = """SELECT json_array_contains('[1, 2, 3]', 2)"""
        result_6 = sqlglot.transpile(input_sql_6, read="presto", write="doris")[0]
        assert (
            result_6 == expected_result_6
        ), f"Transpile result doesn't match expected result. Expected: {expected_result_6}, Actual: {result_6}"
        print("Test6 passed!")

    def test_filter(self):
        expected_result_1 = "SELECT aa, sum(CASE WHEN index_name = 'ceshi' THEN score ELSE 0 END) AS avg_score FROM table GROUP BY aa"
        input_sql_1 = """select aa,sum(score) filter(where index_name='ceshi') as avg_score from table group by aa"""
        result_1 = sqlglot.transpile(input_sql_1, read="presto", write="doris")[0]
        assert (
            result_1 == expected_result_1
        ), f"Transpile result doesn't match expected result. Expected: {expected_result_1}, Actual: {result_1}"
        print("Test6 passed!")

    def test_presto(self):
        expected_result_1 = "SELECT * FROM a WHERE a = ${canc_date}"
        input_sql_1 = """select * from a where a = ${canc_date}"""
        result_1 = sqlglot.transpile(input_sql_1, read="presto", write="doris")[0]
        assert (
                result_1 == expected_result_1
        ), f"Transpile result doesn't match expected result. Expected: {expected_result_1}, Actual: {result_1}"
        print("Test6 passed!")

