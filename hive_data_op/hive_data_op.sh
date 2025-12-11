#!/bin/bash

# 检查参数
if [ $# -lt 2 ]; then
    echo "Usage: $0 <table_name> <ds_value>"
    exit 1
fi

TABLE_NAME=$1
DS_VALUE=$2
BACKUP_TABLE="${TABLE_NAME}_bak"

echo "1. Preparing workspace..."
echo "Target Table: ${TABLE_NAME}"
echo "Backup Table: ${BACKUP_TABLE}"

# 1.1 如果备份表已存在，先删除
echo "Dropping old backup table if exists..."
hive -e "DROP TABLE IF EXISTS ${BACKUP_TABLE};"

# 1.2 检查原表是否存在
hive -S -e "DESCRIBE ${TABLE_NAME}" > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Error: Table ${TABLE_NAME} does not exist."
    exit 1
fi

# 1.3 重命名原表为备份表
echo "Renaming ${TABLE_NAME} to ${BACKUP_TABLE}..."
hive -e "ALTER TABLE ${TABLE_NAME} RENAME TO ${BACKUP_TABLE};"

if [ $? -ne 0 ]; then
    echo "Error: Failed to rename table."
    exit 1
fi

SOURCE_TABLE=$BACKUP_TABLE
TARGET_TABLE=$TABLE_NAME

echo "2. Getting schema from ${SOURCE_TABLE}..."

# 获取表结构，过滤掉表头、WARN日志、空行、分区信息行(#开头)
# 使用 -S (silent) 模式减少非查询输出
RAW_SCHEMA=$(hive -S -e "DESCRIBE ${SOURCE_TABLE}")

if [ $? -ne 0 ]; then
    echo "Error: Failed to describe source table."
    # 尝试恢复原表名
    hive -e "ALTER TABLE ${BACKUP_TABLE} RENAME TO ${TABLE_NAME};"
    exit 1
fi

COLUMNS_DEF=""
SELECT_COLS=""

# 设置 IFS 为换行符，以便逐行读取
IFS=$'\n'
for line in $RAW_SCHEMA; do
    # 去除首尾空格
    line=$(echo "$line" | xargs)
    
    # 跳过空行
    if [ -z "$line" ]; then continue; fi
    
    # 跳过以 # 开头的行 (分区信息)
    if [[ "$line" == \#* ]]; then continue; fi
    
    # 提取字段名和类型 (假设前两列)
    col_name=$(echo "$line" | awk '{print $1}')
    col_type=$(echo "$line" | awk '{print $2}')
    
    # 跳过字段名为 ds 的行 (源表已有 ds，目标表将 ds 作为分区)
    if [ "$col_name" == "ds" ]; then 
        echo "Skipping source 'ds' column..."
        continue 
    fi
    
    # 拼接字段定义和查询字段
    if [ -z "$COLUMNS_DEF" ]; then
        COLUMNS_DEF="\`${col_name}\` ${col_type}"
        SELECT_COLS="\`${col_name}\`"
    else
        COLUMNS_DEF="${COLUMNS_DEF}, \`${col_name}\` ${col_type}"
        SELECT_COLS="${SELECT_COLS}, \`${col_name}\`"
    fi
done
unset IFS

if [ -z "$COLUMNS_DEF" ]; then
    echo "Error: No columns found in source table or parsing failed."
    # 尝试恢复
    hive -e "ALTER TABLE ${BACKUP_TABLE} RENAME TO ${TABLE_NAME};"
    exit 1
fi

echo "3. Creating target table ${TARGET_TABLE}..."

# 使用 ORC 存储和 Snappy 压缩
HQL_CREATE="CREATE TABLE IF NOT EXISTS ${TARGET_TABLE} (
    ${COLUMNS_DEF}
)
PARTITIONED BY (ds string) 
STORED AS ORC 
TBLPROPERTIES('orc.compression'='snappy');"

echo "SQL: $HQL_CREATE"
hive -e "$HQL_CREATE"

if [ $? -ne 0 ]; then
    echo "Error: Failed to create target table."
    # 恢复 (需要先删除可能创建失败的表)
    hive -e "DROP TABLE IF EXISTS ${TABLE_NAME}; ALTER TABLE ${BACKUP_TABLE} RENAME TO ${TABLE_NAME};"
    exit 1
fi

echo "4. Inserting data into ${TARGET_TABLE} partition (ds='${DS_VALUE}')..."

# 保持动态分区设置
HQL_INSERT="
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${TARGET_TABLE} PARTITION (ds='${DS_VALUE}')
SELECT ${SELECT_COLS} FROM ${SOURCE_TABLE};"

echo "SQL: $HQL_INSERT"
hive -e "$HQL_INSERT"

if [ $? -ne 0 ]; then
    echo "Error: Failed to insert data."
    echo "Check backup table: ${BACKUP_TABLE}"
    exit 1
fi

echo "Done. Backup table ${BACKUP_TABLE} is kept for safety."
