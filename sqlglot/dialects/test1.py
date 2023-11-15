import sqlglot
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.qualify_tables import qualify_tables

def main(sql_query, read, write, method):
    if method == 1:
        # with open("all-test-case-smartbi2.sql", "r") as input_file:
        #     sql = input_file.read()
        result = sqlglot.transpile(sql_query, read=read, write=write, pretty=True)[0]
        expression = sqlglot.parse_one(result)
        # 将结果写入文件
        # with open("res.sql", "w") as file:
        #     for item in result:
        #         file.write("%s;\n" % item)
        print(expression)
    elif method == 2:#当有别名不全或者表名大小写需要更改，可以将method设置为2

        res = sqlglot.transpile(sql_query, read=read, write=write, pretty=True)[0]
        expression = sqlglot.parse_one(res, read=read)
        # True 代表将表名改为大写，False 代表将表名改成小写，None 代表不改变表名大小写，默认值为None
        result = qualify_tables(expression, case_sensitive=False).sql()
        print(result)
    elif method == 3:#当有别名(列名)语法时，可以将method设置为3，进行改写
        # 改写列别名
        expression = sqlglot.parse_one(sql_query)
        print(qualify(expression, quote_identifiers=False).sql())
    else:
        print("Invalid method specified.")

sql = """
select toString(toYear( toDate(`S_DATE`) )) as `c0`, toString(toYear( toDate(`S_DATE`) )) || '-' || if(toMonth(toDate(`S_DATE`)) < 10, '0' || toString(toMonth(toDate(`S_DATE`))), toString(toMonth(toDate(`S_DATE`)))) as `c1` from (select T339.`S_DATE`,T339.`divisionId`,T339.`area`,T339.`dept_id`,T339.`ID`,T339.`line_type`,T339.`dept_line`,T339.`dept_id5`,T339.`staff_id`,T339.`viewId` from `smartbimpp`.`Iff808081017d51cc51cce434017d5aeec35e7022` T339 /* RowPermission_start */ where ( (( T339.`divisionId` in( select divison_code from dbo.dm_division_authority where staff_id = 'admin' ) ) ) and (`viewId`='COMBINEDQUERY23' or `viewId`='COMBINEDQUERY26' or `viewId`='COMBINEDQUERY2' or `viewId`='COMBINEDQUERY28' or `viewId`='COMBINEDQUERY8' or `viewId`='COMBINEDQUERY13' or `viewId`='COMBINEDQUERY36' or `viewId`='COMBINEDQUERY37' or `viewId`='COMBINEDQUERY10' or `viewId`='COMBINEDQUERY11' or `viewId`='COMBINEDQUERY16' or `viewId`='COMBINEDQUERY38' or `viewId`='COMBINEDQUERY18') or `viewId` not in('COMBINEDQUERY23','COMBINEDQUERY26','COMBINEDQUERY2','COMBINEDQUERY28','COMBINEDQUERY8','COMBINEDQUERY13','COMBINEDQUERY36','COMBINEDQUERY37','COMBINEDQUERY10','COMBINEDQUERY11','COMBINEDQUERY16','COMBINEDQUERY38','COMBINEDQUERY18')) and ( (( T339.`line_type` in ( select dept_line from dbo.dm_invent_authority where staff_id = 'admin') ) ) and (`viewId`='COMBINEDQUERY23' or `viewId`='COMBINEDQUERY26' or `viewId`='COMBINEDQUERY5' or `viewId`='COMBINEDQUERY9' or `viewId`='COMBINEDQUERY7' or `viewId`='COMBINEDQUERY8' or `viewId`='COMBINEDQUERY13' or `viewId`='COMBINEDQUERY36' or `viewId`='COMBINEDQUERY37' or `viewId`='COMBINEDQUERY10' or `viewId`='COMBINEDQUERY11' or `viewId`='COMBINEDQUERY16' or `viewId`='COMBINEDQUERY38' or `viewId`='COMBINEDQUERY39' or `viewId`='COMBINEDQUERY19') or `viewId` not in('COMBINEDQUERY23','COMBINEDQUERY26','COMBINEDQUERY5','COMBINEDQUERY9','COMBINEDQUERY7','COMBINEDQUERY8','COMBINEDQUERY13','COMBINEDQUERY36','COMBINEDQUERY37','COMBINEDQUERY10','COMBINEDQUERY11','COMBINEDQUERY16','COMBINEDQUERY38','COMBINEDQUERY39','COMBINEDQUERY19')) and ( (( T339.`ID` in( select area_code from dbo.dm_area_authority where staff_id = 'admin' ) ) ) and (`viewId`='COMBINEDQUERY26' or `viewId`='COMBINEDQUERY6' or `viewId`='COMBINEDQUERY9' or `viewId`='COMBINEDQUERY7' or `viewId`='COMBINEDQUERY8' or `viewId`='COMBINEDQUERY13' or `viewId`='COMBINEDQUERY36' or `viewId`='COMBINEDQUERY11' or `viewId`='COMBINEDQUERY16' or `viewId`='COMBINEDQUERY38' or `viewId`='COMBINEDQUERY18' or `viewId`='COMBINEDQUERY19') or `viewId` not in('COMBINEDQUERY26','COMBINEDQUERY6','COMBINEDQUERY9','COMBINEDQUERY7','COMBINEDQUERY8','COMBINEDQUERY13','COMBINEDQUERY36','COMBINEDQUERY11','COMBINEDQUERY16','COMBINEDQUERY38','COMBINEDQUERY18','COMBINEDQUERY19')) and ( (( T339.`dept_id5` in( select dept_id from dbo.dm_dep_authority where staff_id = 'admin' ) ) ) and (`viewId`='COMBINEDQUERY20' or `viewId`='COMBINEDQUERY22' or `viewId`='COMBINEDQUERY27' or `viewId`='COMBINEDQUERY12' or `viewId`='COMBINEDQUERY14' or `viewId`='COMBINEDQUERY15' or `viewId`='COMBINEDQUERY38') or `viewId` not in('COMBINEDQUERY20','COMBINEDQUERY22','COMBINEDQUERY27','COMBINEDQUERY12','COMBINEDQUERY14','COMBINEDQUERY15','COMBINEDQUERY38')) /* RowPermission_end */) as `fact_union_all` group by `c0`, `c1` order by CASE WHEN `c0` IS NULL THEN 1 ELSE 0 END, `c0` ASC, CASE WHEN `c1` IS NULL THEN 1 ELSE 0 END, `c1` ASC

"""

# 使用参数值调用 main 函数，传入相应的参数
# main(sql, "snowflake", "doris", 1)
main(sql, "clickhouse", "doris", 2)
# main(sql, "snowflake", "doris", 3)