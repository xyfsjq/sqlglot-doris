from pydoris.doris_client import *
from pydoris.util.generate_test_data import *

fe_host = "10.16.10.6"
fe_http_port = "8141"
fe_query_port = "9131"
username = 'root'
passwd = ""
db = "tpch"
doris_client = DorisClient(fe_host=fe_host,
                           fe_query_port=fe_query_port,
                           fe_http_port=fe_http_port,
                           username=username,
                           password=passwd,
                           db=db)


def test_create_database():
    return doris_client.create_database('pydoris_client_test')


def test_create_table():
    doris_client.execute("""create table if not exists pydoris_client_test.write_test(
                                   f_id int,
                                   f_decimal decimal(18,6),
                                   f_timestamp bigint,
                                   f_datetime datetime(6),
                                   f_str string,
                                   f_float float,
                                   f_boolean boolean
                                   )duplicate key(`f_id`)
                                   distributed by hash(`f_id`) buckets 1
                                   properties("replication_allocation" = "tag.location.default: 1");""")


def test_get_table_columns():
    print(doris_client.get_table_columns('pydoris_client_test', 'write_test'))


def gen_test_data(num):
    list = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    for i in range(num):
        line = (i,
                generate_decimal(),
                generate_timestamp(),
                generate_random_datetime(start_date, end_date),
                generate_random_string(20),
                generate_float(),
                generate_boolean())
        list.append(line)
    return list


# If your data line delimiter need to be specified，use options.set_line_delimiter(delimiter)
def test_write_csv():
    # print(list)
    df = pd.DataFrame(gen_test_data(100000))
    df.columns = ['f_id', 'f_decimal', 'f_timestamp', 'f_datetime', 'f_str', 'f_float', 'f_boolean']
    # doris_client.options.set_csv_format(",").set_auto_uuid_label().set_line_delimiter("\\n")
    csv = df.to_csv(header=False, index=False)
    doris_client.write("pydoris_client_test.write_test", csv)


# 1. If you need use json format to insert data to Doris, you need set json format ,
#    because the default format is csv format
# 2. When you json data is [{},{}] please set strip_outer_array=true
# 3. You can customize data import labels , use options.set_label(your_label)
def test_write_json():
    df = pd.DataFrame(gen_test_data(100000),
                      columns=['f_id', 'f_decimal', 'f_timestamp', 'f_datetime', 'f_str', 'f_float', 'f_boolean'])
    json_data = df.to_json(orient='records')
    options = WriteOptions()
    options.set_json_format()
    options.set_option("strip_outer_array", "true")
    doris_client.write("pydoris_client_test.write_test", json_data, options=options)


# data_df: pd.DataFrame, table_name: str, table_model: str is must
# When repeat_replacement = True, tables with duplicate names will be deleted，be careful
def test_write_from_df():
    df = pd.DataFrame(gen_test_data(100000),
                      columns=['f_id', 'f_decimal', 'f_timestamp', 'f_datetime', 'f_str', 'f_float', 'f_boolean'])
    doris_client.write_from_df(df, "pydoris_client_test.df_write_test", "UNIQUE", ['f_id'],
                               distributed_hash_key=["f_id"], buckets=1,
                               field_mapping=[("f_decimal", "Decimal(18,6)")]
                               , table_properties={"replication_allocation": "tag.location.default: 1"},
                               repeat_replacement=False)


def test_read_to_df():
    dataframe = doris_client.query_to_dataframe("select * from pydoris_client_test.write_test limit 1000",
                                                ['f_id', 'f_decimal', 'f_timestamp', 'f_datetime',
                                                 'f_str', 'f_float', 'f_boolean'])
    with pd.option_context('expand_frame_repr', False, 'display.max_rows', None):
        print(dataframe)


def test_query(query):
    result = doris_client.query(query)
    print(result)



def test_list_tables():
    tables = doris_client.list_tables("pydoris_client_test")
    print(tables)


def test_drop_table():
    db = 'pydoris_client_test'
    table_name1 = 'write_test'
    table_name2 = 'df_write_test'
    tables = doris_client.list_tables(db)
    print(tables)
    doris_client.drop_table(db, table_name1)
    doris_client.drop_table(db, table_name2)
    tables = doris_client.list_tables(db)
    print(tables)

if __name__ == '__main__':
    # sql = "SELECT col1, col2 FROM table1 AS t1 LEFT JOIN table3 AS t3 ON t1.col1 = t3.col4 UNION ALL SELECT col3, col4 FROM table2"
    # sql = "select count(1),approx_distinct(x1),BITWISE_AND(x2,x3),BITWISE_NOT(x3,x4),CONTAINS(x5,x6) from (select * from A)"
    # sql = "select t.col1 from (select t.col1 from (select * from table1) t union all select t.col2 from (select * from table2) t)t"
    # sql = "select user_id,,sum(cost) filter(where age='20') as avg_score from example_tbl_agg1 group by user_id"
    # sql = "select * from a where a = ${canc_date}"
    sql = "select count(*) from (select * from example_db.example_tbl_agg1);"
    import dorisApi

    transform_sql = dorisApi.transpile(sql, read="presto", write="doris", case_sensitive=False, pretty=False)[0]
    print(transform_sql)
    test_query("set sql_dialect = \"presto\";")
    test_query(transform_sql)
