#!/bin/bash

# 获取脚本所在目录
BASE_DIR=$(cd "$(dirname "$0")"; pwd)
cd "$BASE_DIR"

# 检查参数
if [ $# -lt 2 ]; then
    echo "Usage: $0 <table_name> <target_date> [max_retries]"
    echo "Example: $0 glsx_data_warehouse.ads_test 2025-12-11 4"
    exit 1
fi

TABLE_NAME=$1
TARGET_DATE=$2
MAX_RETRIES=${3:-4} # 默认为 4 次

# 定义日志目录和文件 (../../log)
LOG_DIR="$BASE_DIR/../../log"
mkdir -p "$LOG_DIR"

# 清理 30 天前的日志
find "$LOG_DIR" -type f -name "prejob_*.log" -mtime +30 -exec rm {} \; 2>/dev/null

# 日志文件名包含表名(简化)和日期，避免并发冲突
SIMPLE_TABLE_NAME=$(echo "$TABLE_NAME" | awk -F. '{print $NF}')
LOG_FILE="$LOG_DIR/prejob_${SIMPLE_TABLE_NAME}_${TARGET_DATE}_$(date +%H%M%S).log"

echo "Log output will be written to: $LOG_FILE"

# 所有的输出都追加到日志文件
{
    echo "=================================================="
    echo "Starting Pre-Job Check at $(date)"
    echo "Table: $TABLE_NAME"
    echo "Target Date: $TARGET_DATE"
    echo "Max Retries: $MAX_RETRIES"
    echo "=================================================="

    # 检查 Python 是否安装
    if ! command -v python3 &> /dev/null; then
        if ! command -v python &> /dev/null; then
            echo "Error: Python is not installed or not in PATH."
            exit 1
        else
            PYTHON_CMD=python
        fi
    else
        PYTHON_CMD=python3
    fi

    # 运行检查脚本
    $PYTHON_CMD pre_job_check.py "$TABLE_NAME" "$TARGET_DATE" "$MAX_RETRIES"

    # 检查执行结果
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Pre-Job Check Passed at $(date)."
    else
        echo "Pre-Job Check Failed at $(date)."
    fi
    echo ""
    
    # 退出并返回相应的状态码
    exit $EXIT_CODE

} >> "$LOG_FILE" 2>&1

#写个调用示例
# ./start_prejob_check.sh glsx_data_warehouse.ads_test 2025-12-11 4
