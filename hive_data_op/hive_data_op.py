import sys
import os
# 添加上级目录到 sys.path，以便导入 hive_checker
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hive_checker import HiveChecker

# 配置信息
HIVE_HOST = '192.168.10.3'
HIVE_PORT = 10000
HIVE_USER = 'hadoop'

class HiveDataOperator(HiveChecker):
    def __init__(self, host, port, username, database='default'):
        super().__init__(host, port, username, database)

    def create_table_with_ds(self, source_table, target_table, ds_value):
        """
        创建一个新表，结构与原表一致，但新增 ds 字段 (作为分区字段)
        并从原表导入数据到指定分区
        :param ds_value: 写入到目标表 ds 分区的值 (例如 '2025-01-01')
        """
        if not self.conn:
            self.connect()
        
        cursor = self.conn.cursor()
        try:
            print(f"1. 获取源表 {source_table} 的结构...")
            # 注意: DESCRIBE 输出通常包含 col_name, data_type, comment
            cursor.execute(f"DESCRIBE {source_table}")
            schema_rows = cursor.fetchall()
            
            # 构建建表语句的字段部分
            columns_def = []
            select_columns = []
            
            for row in schema_rows:
                col_name = row[0].strip()
                col_type = row[1].strip()
                
                # 跳过分区信息的分割线 (如果有)
                if not col_name or col_name.startswith('#'):
                    continue
                    
                columns_def.append(f"`{col_name}` {col_type}")
                select_columns.append(f"`{col_name}`")
            
            columns_sql = ", ".join(columns_def)
            select_sql = ", ".join(select_columns)
            
            # 2. 创建目标表 (ds 作为分区字段)
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                {columns_sql}
            )
            PARTITIONED BY (ds string)
            STORED AS PARQUET
            """
            print(f"2. 创建目标表 {target_table}...\nSQL: {create_sql}")
            cursor.execute(create_sql)
            
            # 3. 导入数据
            insert_sql = f"""
            INSERT INTO TABLE {target_table} PARTITION (ds='{ds_value}')
            SELECT {select_sql} FROM {source_table}
            """
            print(f"3. 从 {source_table} 导入数据到 {target_table} (分区 ds='{ds_value}')...")
            print(f"SQL: {insert_sql}")
            
            # 执行插入 (这可能比较耗时)
            cursor.execute(insert_sql)
            print("数据导入完成。")
            
        except Exception as e:
            print(f"操作失败: {e}")
            raise
        finally:
            cursor.close()

if __name__ == "__main__":
    # 示例用法
    # python hive_data_op.py source_db.source_tbl target_db.target_tbl 2025-01-01
    if len(sys.argv) < 4:
        print("Usage: python hive_data_op.py <source_table> <target_table> <ds_value>")
        sys.exit(1)
        
    source_tbl = sys.argv[1]
    target_tbl = sys.argv[2]
    ds_val = sys.argv[3]
    
    op = HiveDataOperator(HIVE_HOST, HIVE_PORT, HIVE_USER)
    try:
        op.create_table_with_ds(source_tbl, target_tbl, ds_val)
    finally:
        op.close()
