import sqlglot
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.qualify_tables import qualify_tables
import sqlparse as sqlparse


def process_query(query, read, case_sensitive, quote_identifiers):
    expression = sqlglot.parse_one(query, read=read)

    if case_sensitive == '1':
        return qualify_tables(expression, case_sensitive=True).sql() + ';'
    elif case_sensitive == '2':
        return qualify_tables(expression, case_sensitive=False).sql() + ';'
    elif quote_identifiers:
        return qualify(expression, quote_identifiers=False).sql() + ';'
    else:
        return query + ';'


def main(sql_query, read, write, source, case_sensitive, quote_identifiers):
    result = ''
    queries = sqlglot.transpile(sql_query, read=read, write=write)
    if source == 'text':
        result = process_query(queries[0], read, case_sensitive, quote_identifiers)
        result = sqlparse.format(result, reindent_aligned=True)
    elif source == 'file':
        result = ''.join(process_query(query, read, case_sensitive, quote_identifiers) for query in queries)
        result = sqlparse.format(result, reindent=True)
    else:
        print("Invalid method specified.")

    print(result)





sql = """

SELECT database, 
       (sum(data_uncompressed_bytes) / total_bytes) * 100 AS database_disk_usage
FROM system.columns
GROUP BY database
ORDER BY database_disk_usage DESC
"""


main(sql, "clickhouse", "doris", 'text', '0', False)
