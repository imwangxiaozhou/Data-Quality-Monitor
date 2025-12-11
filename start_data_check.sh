#!/bin/bash

# 获取脚本所在目录
BASE_DIR=$(cd "$(dirname "$0")"; pwd)
cd "$BASE_DIR"

# 定义日志目录和文件 (../../log/monitor_task/)
LOG_DIR="$BASE_DIR/../../log/monitor_task"
#还要创建一个文件夹
mkdir -p "$LOG_DIR"

# 清理 30 天前的日志
find "$LOG_DIR" -type f -name "monitor_*.log" -mtime +30 -exec rm {} \; 2>/dev/null

LOG_FILE="$LOG_DIR/monitor_$(date +%Y%m%d).log"

# 所有的输出都追加到日志文件
{
    echo "=================================================="
    echo "Starting Data Quality Monitor at $(date)"
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

    # 运行监控脚本
    $PYTHON_CMD monitor_task.py

    # 检查执行结果
    if [ $? -eq 0 ]; then
        echo "Monitor task completed successfully at $(date)."
    else
        echo "Monitor task failed at $(date)."
        exit 1
    fi
    echo "" # 空行分隔
} >> "$LOG_FILE" 2>&1

#写个调用示例
# ./start_data_check.sh
