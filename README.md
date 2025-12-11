# Data Quality Monitor (数据质量监控项目)

本项目主要用于监控 Hive 数仓的数据质量，包括前置任务检查和日常数据质量监控，并通过企业微信推送告警。

## 📁 目录结构与文件说明

| 文件名 | 作用描述 |
| --- | --- |
| `monitor_task.py` | **核心监控脚本**。每日运行，检查指定 Hive 表的数据时效、数据量和 `status` 字段状态。生成 Markdown 报告和 CSV 明细推送到企业微信。会自动清理 30 天前的报告。 |
| `start_data_check.sh` | **监控任务启动脚本** (Shell)。用于调度系统调用，负责环境检查、日志记录和日志清理 (保留30天)。 |
| `pre_job_check.py` | **前置任务检查脚本**。用于在 ETL 任务开始前，检查依赖表是否已产出。支持失败重试 (默认4次，每次间隔5分钟)。 |
| `start_prejob_check.sh` | **前置检查启动脚本** (Shell)。用于调度系统调用，支持传参 (表名、日期、重试次数)，并记录独立日志。 |
| `hive_checker.py` | **Hive 操作工具类**。封装了连接 Hive (兼容 Thrift 0.11)、查询最大分区、查询明细、检查字段分布等通用方法。 |
| `wechat_sender.py` | **企业微信发送工具类**。封装了发送 Markdown 消息、上传文件和发送文件的功能。 |
| `hql_test.py` | **SQL 测试脚本**。用于手动测试 HQL 语句，验证连接和查询结果。 |
| `requirements.txt` | **项目依赖文件**。包含 `pyhive`, `thrift` (0.11.0), `requests` 等库的版本信息。 |
| `reports/` | **报告目录**。存放每日生成的 CSV 明细文件。 |
| `log/` | **日志目录** (位于项目上级目录)。存放脚本运行日志。 |

## 🚀 使用指南

### 1. 日常数据质量监控
通常配置在每日调度任务中 (如每天早上 9:00)。
```bash
sh start_data_check.sh
```

### 2. 前置任务检查
配置在具体 ETL 任务之前，作为依赖检查。
```bash
# 用法: sh start_prejob_check.sh <表名> <目标日期> [重试次数]
sh start_prejob_check.sh glsx_data_warehouse.ads_some_table 2025-12-11 4
```

### 3. 环境部署
```bash
pip install -r requirements.txt
```
