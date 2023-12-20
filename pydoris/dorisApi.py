from __future__ import annotations
from sqlglot.dialects.dialect import DialectType
from sqlglot.optimizer.qualify_tables import qualify_tables
import logging
import typing as t
from sqlglot.dialects.dialect import Dialect as Dialect, Dialects as Dialects
from sqlglot.errors import (
    ErrorLevel as ErrorLevel,
)
from sqlglot.expressions import (
    Expression as Expression,
)
if t.TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType as DialectType
logger = logging.getLogger("sqlglot")


def parse(
    sql: str, read: DialectType = None, dialect: DialectType = None, **opts
) -> t.List[t.Optional[Expression]]:
    """
    Parses the given SQL string into a collection of syntax trees, one per parsed SQL statement.

    Args:
        sql: the SQL code string to parse.
        read: the SQL dialect to apply during parsing (eg. "spark", "hive", "presto", "mysql").
        dialect: the SQL dialect (alias for read).
        **opts: other `sqlglot.parser.Parser` options.

    Returns:
        The resulting syntax tree collection.
    """
    return Dialect.get_or_raise(read or dialect).parse(sql, **opts)


def transpile(
    sql: str,
    read: DialectType = None,
    write: DialectType = None,
    identity: bool = True,
    error_level: t.Optional[ErrorLevel] = None,
    case_sensitive: t.Optional[bool] = None,
    **opts,
) -> t.List[str]:
    """
    Parses the given SQL string in accordance with the source dialect and returns a list of SQL strings transformed
    to conform to the target dialect. Each string in the returned list represents a single transformed SQL statement.

    Args:
        sql: the SQL code string to transpile.
        read: the source dialect used to parse the input string (eg. "spark", "hive", "presto", "mysql").
        write: the target dialect into which the input should be transformed (eg. "spark", "hive", "presto", "mysql").
        identity: if set to `True` and if the target dialect is not specified the source dialect will be used as both:
            the source and the target dialect.
        error_level: the desired error level of the parser.
        **opts: other `sqlglot.generator.Generator` options.

    Returns:
        The list of transpiled SQL statements.
    """
    write = (read if write is None else write) if identity else write
    write = Dialect.get_or_raise(write)
    return [
        write.generate(qualify_tables(expression,case_sensitive=case_sensitive), copy=False, **opts) if expression else ""
        for expression in parse(sql, read, error_level=error_level)
    ]

if __name__ == '__main__':
    # sql = "SELECT col1, col2 FROM table1 AS t1 LEFT JOIN table3 AS t3 ON t1.col1 = t3.col4 UNION ALL SELECT col3, col4 FROM table2"
    # sql = "select count(1),approx_distinct(x1),BITWISE_AND(x2,x3),BITWISE_NOT(x3,x4),CONTAINS(x5,x6) from (select * from A)"
    # sql = "select t.col1 from (select t.col1 from (select * from table1) t union all select t.col2 from (select * from table2) t)t"
    # sql = "select user_id,,sum(cost) filter(where age='20') as avg_score from example_tbl_agg1 group by user_id"
    # sql = "select * from a where a = ${canc_date}"
    sql = "select a||b"
    # sql = """select trim(to_char(CustomerCode,'99999999'))  AS CustomerCode"""
    print(transpile(sql,read="presto",write="doris",case_sensitive=False,pretty=False)[0])
