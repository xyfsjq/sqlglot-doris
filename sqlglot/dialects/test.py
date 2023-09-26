import sqlglot

from sqlglot.optimizer.qualify_tables import qualify_tables

sql = """    
	SELECT DATEDIFF(hour, '2013-05-08 23:39:20'::TIMESTAMP, 
    '2015-05-08 23:39:20'::TIMESTAMP)
               AS diff_hours;
"""
expression = sqlglot.parse_one(sql, read="snowflake")
res = qualify_tables(expression).sql()
result = sqlglot.transpile(res, read="snowflake", write="doris", pretty=True)[0]
print(result)
# formatted_sql = sqlparse.format(res, reindent=True, indent_tabs=False)
# print(formatted_sql)

# def main():
#     input_file_path = "test_clickhouse.sql"
#     output_file_path = "test_clickhouse_result.sql"
#
#     # 读取 SQL 文件并按分号分行
#     with open(input_file_path, "r") as input_file:
#         sql_statements = input_file.read().split(";")
#
#     # 进行转换并写入结果集文件
#     with open(output_file_path, "w") as output_file:
#         for sql in sql_statements:
#             if sql.strip():  # 跳过空语句
#                 transformed_sql = transform_sql(sql)  # 自定义的转换函数
#                 output_file.write(transformed_sql + "\n")
#
#     print("转换完成，结果已写入", output_file_path)
#
#
# def transform_sql(sql):
#     # 在这里编写你的 SQL 转换逻辑
#     # 例如，可以对 SQL 进行替换、修改、添加等操作
#     # 返回转换后的 SQL
#     transformed_sql = sqlglot.transpile(sql, read="clickhouse", write="doris")[0]  # 示例：将 clickhouse SQL 转换doris SQL
#     return transformed_sql
#
#
# if __name__ == "__main__":
#     main()
