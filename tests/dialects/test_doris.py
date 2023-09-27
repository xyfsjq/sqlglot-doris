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
