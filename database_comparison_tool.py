# -*- coding: utf-8 -*-
"""
@Time ： 2023/6/16 16:29
@Auth ： 胡英俊(请叫我英俊)
@File ：database_comparison_tool.py
@IDE ：PyCharm
@Motto：Never stop learning
"""
import time
import datetime
import pymysql
import traceback

"""数据库比对脚本"""


def add_or_modify_column_sql(is_add, table_name, column_name, column_type, null_able, default_value, comment):
    """获取新增或者修改的字段SQL"""
    null_able = "null" if null_able == "YES" else " not null "
    add_sql = "alter table {} add column {} {} {}".format(table_name, column_name, column_type, null_able)
    if default_value is not None and len(default_value) > 0:
        add_sql += " default" + default_value
    if comment is not None and len(comment) > 0:
        add_sql += " comment '{}'".format(comment)
    if is_add:
        add_sql = add_sql.replace("add column", "modify column")
    add_sql += ";"
    return add_sql


def decode_obj(s):
    if type(s) == bytes or type(s) == bytearray:
        return s.decode()
    return s


class DatabaseCompare:
    def __init__(self):
        self.__sourceDb = None
        self.__targetDb = None
        # 获取数据库表，设置过滤前缀
        self.__getCurrentDbTableSql = "select table_name from information_schema.tables where " \
                                      "table_schema=database() "

        self.__getTableColumnSql = "select column_name,column_type,is_nullable,column_default,column_comment  " \
                                   "from information_schema.columns where table_schema=database() " \
                                   "and column_name != 'id' and table_name = '{}'"

    def source_db_connect(self, **kwargs):
        """连接源数据库"""
        config = kwargs.copy()
        self.__sourceDb = pymysql.connect(
            host = config["host"], # 数据库主机IP
            port = config["port"], # 数据库服务端口
            db = config["db"], # 库名
            user = config["user"], # 用户名
            password = config["password"], # 密码
        )
        self.cursor = self.__sourceDb.cursor()

    def target_db_connect(self, **kwargs):
        """连接目标数据库"""
        config = kwargs.copy()
        self.__targetDb = pymysql.connect(
            host = config["host"], # 数据库主机IP
            port = config["port"], # 数据库服务端口
            db = config["db"], # 库名
            user = config["user"], # 用户名
            password = config["password"], # 密码
        )
        self.cursor = self.__targetDb.cursor()

    def filter_table_prefix(self, tuples):
        """指定表前缀过滤"""
        if tuples is not None and len(tuples) > 0:
            sql = "and ("
            for v in tuples:
                sql += " table_name like '{}%' or".format(v)
            sql = sql[0:len(sql) - 3]
            sql += ")"
            self.__getCurrentDbTableSql += sql
        print("print filter table prefix sql: {}".format(self.__getCurrentDbTableSql))

    def filter_table_name(self, tuples):
        """指定表"""
        if tuples is not None and len(tuples) > 0:
            sql = " and table_name in("
            for v in tuples:
                sql += "'{}', ".format(v)
            sql = sql[0: len(sql) - 2]
            sql += ")"
            self.__getCurrentDbTableSql += sql
        print("print filter table sql: {}".format(self.__getCurrentDbTableSql))


    def bind_source_db_connect(self):
        """源数据库连接"""
        return self.__sourceDb

    def bind_target_db_connect(self):
        """目标数据库连接"""
        return self.__targetDb

    def db_close(self):
        self.bind_source_db_connect().close()
        self.bind_target_db_connect().close()

    def get_source_db_all_table_name(self):
        """源数据库所有表名"""
        try:
            cursor = self.bind_source_db_connect().cursor()
            cursor.execute(self.__getCurrentDbTableSql)
            result_list = cursor.fetchall()
            return result_list
        except Exception as e:
            print("发生异常 ", e)

    def get_target_db_all_table_name(self):
        """目标数据库所有表名"""
        try:
            cursor = self.bind_target_db_connect().cursor()
            cursor.execute(self.__getCurrentDbTableSql)
            result_list = cursor.fetchall()
            return result_list
        except Exception as e:
            print("发生异常 ", e)

    def get_difference_table_name(self):
        """获取差异的表"""
        source_list = self.get_source_db_all_table_name()
        target_list = self.get_target_db_all_table_name()
        return list(set(source_list).difference(set(target_list)))

    def get_difference_table_create_sql(self):
        """获取差异建表SQL"""
        difference_table_list = self.get_difference_table_name()
        result_list = []
        if len(difference_table_list) > 0:
            try:
                cursor = self.bind_source_db_connect().cursor()
                for table in difference_table_list:
                    cursor.execute("show create table " + table[0])
                    result = cursor.fetchone()
                    result_list.append(str(result[1]).replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS") + ";")
            except Exception as e:
                print("发生异常", e)
            print("difference table count {}".format(len(result_list)))
            return result_list

    def get_difference_table_column_sql(self):
        """获取相同表字段差异SQL"""
        source_list = self.get_source_db_all_table_name()
        target_list = self.get_target_db_all_table_name()
        intersection_list = set(source_list).intersection(set(target_list))
        result_list = list()
        if len(intersection_list) > 0:
            try:
                source_cursor = self.bind_source_db_connect().cursor()
                target_cursor = self.bind_target_db_connect().cursor()
                # 循环获取表字段属性
                for table in intersection_list:
                    # 源库
                    source_cursor.execute(self.__getTableColumnSql.format(table[0]))
                    source_columns = source_cursor.fetchall()
                    # 目标库
                    target_cursor.execute(self.__getTableColumnSql.format(table[0]))
                    target_columns = target_cursor.fetchall()
                    # 循环校验字段属性
                    for source_column in source_columns:
                        target_column = None
                        column_name = decode_obj(source_column[0])
                        column_type = decode_obj(source_column[1])
                        null_able = decode_obj(source_column[2])
                        default_value = decode_obj(source_column[3])
                        comment = decode_obj(source_column[4])
                        for field in target_columns:
                            if column_name == field[0]:
                                target_column = field
                                break
                        # 判断相同字段属性不同
                        if target_column is not None:
                            if column_name != decode_obj(target_column[0]) or column_type != decode_obj(target_column[1]) \
                                    or null_able != decode_obj(target_column[2]) or default_value != decode_obj(target_column[3]) \
                                    or comment != decode_obj(target_column[4]):
                                result_list.append(add_or_modify_column_sql(True, table[0], column_name, column_type, null_able, default_value, comment))
                        else: # 不存在的字段
                            result_list.append(add_or_modify_column_sql(False, table[0], column_name, column_type, null_able, default_value, comment))
            except Exception as e:
                print("发生异常", e)
                traceback.print_exc()
            print("difference column count {}".format(len(result_list)))
            return result_list

    def export_difference_sql_file(self, source_path, target_path, export_type):
        if export_type is not None and export_type == 1:
            print("开始导出差异表和表字段SQL ==========>>>> ")
        elif export_type is not None and export_type == 2:
            print("开始导出差异表和表字段SQL ==========>>>> ")
        elif export_type is not None and export_type == 3:
            print("开始导出差异表和表字段SQL ==========>>>> ")
        start_time = datetime.datetime.now()
        try:
            if export_type is not None and (export_type == 1 or export_type == 2):
                result = self.get_difference_table_create_sql()
                fp = open(source_path, "a")
                if result is not None:
                    for res in result:
                        fp.write(res + "\n")
                    fp.close()
                    print("Export Success: table file: {}".format(source_path))

            if export_type is not None and (export_type == 1 or export_type == 3):
                result = self.get_difference_table_column_sql()
                if result is not None:
                    fp = open(target_path, "a")
                    for res in result:
                        try:
                            fp.write(res + "\n")
                        except Exception as e:
                            print(e)
                    fp.close()
                    print("Export Success: table file: {}".format(target_path))
        except Exception as e:
            print(e)
        finally:
            self.db_close()
            print("总耗时：{}".format((datetime.datetime.now() - start_time).total_seconds()))



if __name__ == "__main__":
    today = time.strftime("%Y%m%d%H%M", time.localtime(time.time()))
    creat_table_sql_file_path = "E:\\work\\Data\\createTableSql-{}.sql".format(today)
    table_column_sql_file_path = "E:\\work\\Data\\tableColumnSql-{}.sql".format(today)
    # print(creat_table_sql_file_path, table_column_sql_file_path)

    # 实例化数据库比对工具类
    tool = DatabaseCompare()
    # 过滤指定表前缀
    # tool.filter_table_prefix(["act_"])
    # 过滤指定表
    # tool.filter_table_name([""])
    tool.source_db_connect(
        host = "" ,
        port = ,
        db = "",
        user = "",
        password = ""
    )

    tool.target_db_connect(
        host = "" ,
        port = ,
        db = "",
        user = "",
        password = "",
    )

    # 导出差异表： 1.全部、2.表、3.字段
    tool.export_difference_sql_file(creat_table_sql_file_path, table_column_sql_file_path,1)
































