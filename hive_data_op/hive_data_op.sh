#!/bin/bash

# 检查参数
if [ $# -lt 3 ]; then
    echo "Usage: $0 <source_table> <target_table> <ds_value>"
    exit 1
fi

SOURCE_TABLE=$1
TARGET_TABLE=$2
DS_VALUE=$3

echo "1. Getting schema from ${SOURCE_TABLE}..."

# 获取表结构，过滤掉表头、WARN日志、空行、分区信息行(#开头)
# 使用 -S (silent) 模式减少非查询输出
RAW_SCHEMA=$(hive -S -e "DESCRIBE ${SOURCE_TABLE}")

if [ $? -ne 0 ]; then
    echo "Error: Failed to describe source table."
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
    exit 1
fi

echo "2. Creating target table ${TARGET_TABLE}..."

HQL_CREATE="CREATE TABLE IF NOT EXISTS ${TARGET_TABLE} (
    ${COLUMNS_DEF}
)
PARTITIONED BY (ds string) stored as orc tblproperties('orc.compression'='snappy');"

echo "SQL: $HQL_CREATE"
hive -e "$HQL_CREATE"

if [ $? -ne 0 ]; then
    echo "Error: Failed to create target table."
    exit 1
fi

echo "3. Inserting data into ${TARGET_TABLE} partition (ds='${DS_VALUE}')..."

HQL_INSERT="
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${TARGET_TABLE} PARTITION (ds='${DS_VALUE}')
SELECT ${SELECT_COLS} FROM ${SOURCE_TABLE};"

echo "SQL: $HQL_INSERT"
hive -e "$HQL_INSERT"

if [ $? -ne 0 ]; then
    echo "Error: Failed to insert data."
    exit 1
fi

echo "Done."
