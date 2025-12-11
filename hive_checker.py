from pyhive import hive
import sys

class HiveChecker:
    def __init__(self, host, port, username, database='default'):
        self.host = host
        self.port = port
        self.username = username
        self.database = database
        self.conn = None

    def connect(self):
        """建立 Hive 连接"""
        try:
            # auth='NOSASL' 通常用于无密码的 Hadoop/Hive 环境
            # 如果 NOSASL 失败 (TSocket read 0 bytes)，且 pyhive 不支持 'PLAIN'，尝试 'NONE'
            self.conn = hive.Connection(
                host=self.host,
                port=self.port,
                username=self.username,
                # auth='NOSASL', 
                auth='NONE',
                database=self.database
            )
            print(f"成功连接到 Hive: {self.host}:{self.port}")
        except Exception as e:
            print(f"连接 Hive 失败: {e}")
            raise

    def get_latest_partition_info(self, table_name, min_ds=None):
        """
        查询指定表最新分区的信息
        :param table_name: 表名
        :param min_ds: 最小日期过滤 (ds > min_ds)，格式 YYYY-MM-DD
        返回: (max_ds, count)
        """
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        try:
            # 1. 获取最大分区
            if min_ds:
                sql_max_ds = f"SELECT max(ds) FROM {table_name} WHERE ds > '{min_ds}'"
            else:
                sql_max_ds = f"SELECT max(ds) FROM {table_name}"
                
            print(f"[{table_name}] 正在查询最大分区: {sql_max_ds}")
            cursor.execute(sql_max_ds)
            result = cursor.fetchone()
            max_ds = result[0] if result else None
            
            if not max_ds:
                return None, 0

            # 2. 获取该分区的数据量
            sql_count = f"SELECT count(1) FROM {table_name} WHERE ds = '{max_ds}'"
            print(f"[{table_name}] 正在查询数据量: {sql_count}")
            cursor.execute(sql_count)
            result_count = cursor.fetchone()
            count = result_count[0] if result_count else 0
            
            return max_ds, count

        except Exception as e:
            print(f"查询失败: {e}")
            return None, 0
        finally:
            cursor.close()

    def get_partition_data(self, table_name, ds):
        """
        查询指定分区的所有明细数据
        :param table_name: 表名
        :param ds: 分区日期
        :return: (columns, data) 列名列表和数据列表
        """
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        try:
            sql = f"SELECT * FROM {table_name} WHERE ds = '{ds}'"
            print(f"[{table_name}] 正在查询明细数据: {sql}")
            cursor.execute(sql)
            
            # 获取列名 (pyhive 返回的 cursor.description 中的列名通常是 'table_name.column_name' 格式)
            # 我们只需要 column_name
            columns = []
            if cursor.description:
                for col in cursor.description:
                    col_name = col[0]
                    if '.' in col_name:
                        col_name = col_name.split('.')[-1]
                    columns.append(col_name)
                    
            # 获取所有数据
            data = cursor.fetchall()
            
            return columns, data
        except Exception as e:
            print(f"查询明细失败: {e}")
            return [], []
        finally:
            cursor.close()

    def check_status_distribution(self, table_name, ds):
        """
        检查指定分区的 status 字段分布情况
        :param table_name: 表名
        :param ds: 分区日期
        :return: (has_status_field, is_abnormal, message)
                 has_status_field: 是否包含 status 字段
                 is_abnormal: 是否异常 (全部为1或全部为2)
                 message: 描述信息
        """
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        try:
            # 1. 检查是否存在 status 字段
            # 注意：Hive 的 DESCRIBE 输出格式可能因版本而异，这里更稳妥的方式是直接尝试查询
            # 或者先查询一条数据看看列名 (前面 get_partition_data 已经做过，但为了解耦这里独立处理)
            
            # 尝试查询 status 字段的去重值
            sql = f"SELECT distinct status FROM {table_name} WHERE ds = '{ds}'"
            print(f"[{table_name}] 正在检查 status 分布: {sql}")
            cursor.execute(sql)
            results = cursor.fetchall()
            
            if not results:
                # 可能是没数据，也可能是字段不存在（但通常字段不存在会在 execute 时抛错）
                # 如果是空数据，前面 count=0 已经拦截了，这里假定有数据
                return True, False, "无 status 数据"

            # 提取所有去重后的 status 值
            status_values = [row[0] for row in results]
            status_set = set(status_values)
            
            # 检查是否只有 1 或只有 2
            # 注意 status 类型可能是 int 或 string，做兼容处理
            status_set_str = {str(v) for v in status_set}
            
            if status_set_str == {'1'}:
                return True, True, "status 字段值全部为 1"
            elif status_set_str == {'2'}:
                return True, True, "status 字段值全部为 2"
            else:
                return True, False, f"status 分布正常 (包含: {status_set_str})"

        except Exception as e:
            # 如果报错包含 "Column 'status' not found" 或类似信息，说明没有 status 字段
            # 简单的判断方式是看异常信息
            error_msg = str(e).lower()
            if "column" in error_msg and "status" in error_msg and "not found" in error_msg:
                 print(f"[{table_name}] 不存在 status 字段，跳过检查")
                 return False, False, "无 status 字段"
            elif "semanticexception" in error_msg and "status" in error_msg: # Hive 常见的列不存在错误
                 print(f"[{table_name}] 不存在 status 字段，跳过检查")
                 return False, False, "无 status 字段"
            
            print(f"[{table_name}] status 检查失败 (可能无此字段): {e}")
            return False, False, f"status 检查出错: {e}"
        finally:
            cursor.close()

    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()

if __name__ == "__main__":
    # 配置信息
    HOST = '192.168.10.3'
    PORT = 10000
    USER = 'hadoop'
    # 注意：表名包含库名，所以这里 database 设为 default 即可，或者直接在 SQL 中指定全限定名
    TABLE = 'glsx_data_warehouse.ads_black_abnormal_area_zl_res'

    checker = HiveChecker(HOST, PORT, USER)
    try:
        max_ds = checker.get_max_ds(TABLE)
        print(f"查询结果 - 表 {TABLE} 的最大 ds 值为: {max_ds}")
    except Exception as e:
        print(f"程序执行出错: {e}")
    finally:
        checker.close()
