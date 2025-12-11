from hive_checker import HiveChecker

# 配置信息
HIVE_HOST = '192.168.10.3'
HIVE_PORT = 10000
HIVE_USER = 'hadoop'

def execute_custom_sql(sql):
    """
    执行自定义 SQL 并打印结果
    :param sql: 要执行的 HQL 语句
    """
    checker = HiveChecker(HIVE_HOST, HIVE_PORT, HIVE_USER)
    
    print(f"准备执行 SQL:\n{sql}\n")
    print("-" * 50)
    
    try:
        if not checker.conn:
            checker.connect()
            
        cursor = checker.conn.cursor()
        cursor.execute(sql)
        
        # 获取列名
        columns = []
        if cursor.description:
            columns = [col[0].split('.')[-1] for col in cursor.description]
            print(f"列名: {columns}")
            
        # 获取结果
        results = cursor.fetchall()
        print(f"查询到 {len(results)} 条记录:")
        
        for i, row in enumerate(results):
            print(f"Row {i+1}: {row}")
            
            # 如果只想看前几条，可以在这里 break
            if i >= 9:
                print("... (只显示前 10 条)")
                break
                
    except Exception as e:
        print(f"SQL 执行出错: {e}")
    finally:
        checker.close()

if __name__ == "__main__":
    # 在这里编写你的 SQL 语句
    my_sql = """
    select* from(SELECT sn,
    bgtime as stay_begin_time,
    addr as stay_address,
    edtime as stay_end_time,
    duration as duration_time,
    lng as stay_longitude,
    lat as stay_latitude,
    prov as stay_province,
    city as stay_city,
    country as stay_country,
    row_number() over(partition by sn order by bgtime desc) as rn
    FROM glsx_data_warehouse.t_gps_stay
    WHERE  day >= '2025-12-09'
    AND (lng IS NOT NULL
    AND lng <>'')
    AND (lat IS NOT NULL
    AND lat <>'') 
    AND duration/3600 >=1) A
    WHERE A.rn = 1 and A.sn='96310200907'
    """

    my_sql = """
    SELECT sn,
    bgtime as stay_begin_time,
    addr as stay_address,
    edtime as stay_end_time,
    duration as duration_time,
    lng as stay_longitude,
    lat as stay_latitude,
    prov as stay_province,
    city as stay_city,
    country as stay_country
    FROM glsx_data_warehouse.t_gps_stay
    WHERE  day >= '2025-12-09'
    AND (lng IS NOT NULL
    AND lng <>'')
    AND (lat IS NOT NULL
    AND lat <>'') 
    AND duration/3600 >=1
    AND sn='96310200907'
    """
    
    execute_custom_sql(my_sql)
