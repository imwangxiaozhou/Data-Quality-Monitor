import datetime
import csv
import os
import re
from hive_checker import HiveChecker
from wechat_sender import WeChatSender

# 配置信息
HIVE_HOST = '192.168.10.3'
HIVE_PORT = 10000
HIVE_USER = 'hadoop'

# 目标表列表
TARGET_TABLES = [
    "glsx_data_warehouse.ads_black_abnormal_area_zl_res",
    "glsx_data_warehouse.ads_zlgj_24hour_offline_black_area_res",
    "glsx_data_warehouse.ads_zlgj_24hour_stay_black_area_res",
    "glsx_data_warehouse.ads_zlgj_48hour_offline_black_area_res",
    "glsx_data_warehouse.ads_zlgj_48hour_stay_black_area_res",
    "glsx_data_warehouse.ads_zlgj_offline_warning_black_area_res",
    "glsx_data_warehouse.ads_zlgj_stay_warning_black_area_res"
]

# 企业微信 Webhook
WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=d741ee77-b177-4f92-b478-e5357cadf990"

def get_date_str(days_offset=0):
    """获取指定偏移量的日期字符串 (YYYY-MM-DD)"""
    return (datetime.datetime.now() - datetime.timedelta(days=days_offset)).strftime('%Y-%m-%d')

def clean_old_reports(days=30):
    """清理指定天数之前的报告文件"""
    report_dir = os.path.join(os.getcwd(), "reports")
    if not os.path.exists(report_dir):
        return
        
    print(f"开始清理 {days} 天前的报告文件...")
    threshold_date = datetime.datetime.now() - datetime.timedelta(days=days)
    
    for filename in os.listdir(report_dir):
        # 匹配 detail_report_YYYY-MM-DD.csv
        match = re.search(r'detail_report_(\d{4}-\d{2}-\d{2})\.csv', filename)
        if match:
            file_date_str = match.group(1)
            try:
                file_date = datetime.datetime.strptime(file_date_str, '%Y-%m-%d')
                if file_date < threshold_date:
                    file_path = os.path.join(report_dir, filename)
                    os.remove(file_path)
                    print(f"已删除过期报告: {filename}")
            except ValueError:
                continue

def check_table_status_detail(max_ds, count):
    """
    检查表状态 (返回详细检查项)
    :param max_ds: 最大分区日期
    :param count: 数据量
    :return: 检查项列表
    """
    today = get_date_str(0)
    yesterday = get_date_str(1)
    
    checks = []
    
    # 1. 数据时效检查
    is_date_valid = max_ds in [today, yesterday]
    date_msg = f"{max_ds}" if max_ds else "NULL"
    if not is_date_valid:
        date_msg += " (滞后)"
    checks.append({
        "name": "数据时效",
        "passed": is_date_valid,
        "msg": date_msg
    })
    
    # 2. 数据量检查
    is_count_valid = count > 0
    count_msg = f"{count}条"
    if not is_count_valid:
        count_msg += " (异常)"
    checks.append({
        "name": "数据量",
        "passed": is_count_valid,
        "msg": count_msg
    })
    
    return checks

def save_details_to_csv(all_data, filename):
    """保存明细数据到 CSV 文件"""
    if not all_data:
        print("没有明细数据需要保存")
        return None
        
    # 提取所有出现的列名，保持顺序
    # 假设 '作业来源' 放在第一列
    fieldnames = ['作业来源']
    seen_fields = set(fieldnames)
    
    for row in all_data:
        for key in row.keys():
            if key not in seen_fields:
                fieldnames.append(key)
                seen_fields.add(key)
                
    # 确保目录存在
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    try:
        with open(filename, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_data)
        print(f"明细报告已保存至: {filename}")
        return filename
    except Exception as e:
        print(f"保存 CSV 失败: {e}")
        return None

def run_monitor():
    # 先清理过期报告
    clean_old_reports()

    checker = HiveChecker(HIVE_HOST, HIVE_PORT, HIVE_USER)
    results = []
    all_details = [] # 用于存储所有表的明细数据
    
    print("开始执行数据质量监控...")
    try:
        # 计算过滤日期 (当前日期 - 3天)
        three_days_ago = get_date_str(3)
        print(f"查询过滤条件: ds > {three_days_ago}")

        for table in TARGET_TABLES:
            # 去掉库名显示，保持简洁
            short_table_name = table.split('.')[-1]
            
            print(f"正在检查表: {short_table_name}")
            # 传入 min_ds 参数
            max_ds, count = checker.get_latest_partition_info(table, min_ds=three_days_ago)
            
            # 获取基础检查项
            checks = check_table_status_detail(max_ds, count)
            
            # 状态分布检查 (如果基础检查通过且有数据)
            # 默认状态检查通过
            status_check = {
                "name": "数据状态",
                "passed": True,
                "msg": "正常"
            }
            
            # 如果前面有失败，或者没数据，可能无法检查状态，或者状态检查也视为不通过(视情况而定)
            # 这里逻辑：如果有数据，就去查状态；如果没有数据，状态检查显示为"无数据跳过"或者包含在数据量检查里
            
            if count > 0:
                has_status, is_abnormal, dist_msg = checker.check_status_distribution(table, max_ds)
                if has_status:
                    if is_abnormal:
                        status_check["passed"] = False
                        status_check["msg"] = dist_msg # 如 "status 字段值全部为 1"
                    else:
                        status_check["msg"] = "正常" # 显式覆盖
                else:
                     status_check["msg"] = "无 status 字段" # 可选，视需求是否作为通过
            else:
                status_check["msg"] = "-"

            checks.append(status_check)
            
            # 汇总该表是否整体健康
            is_healthy = all(c['passed'] for c in checks)
            
            results.append({
                "table": short_table_name,
                "checks": checks,
                "is_healthy": is_healthy
            })
            
            # 如果有数据，查询明细并汇总
            if max_ds and count > 0:
                cols, data = checker.get_partition_data(table, max_ds)
                for row in data:
                    # 将 row (tuple) 转为 dict，并添加来源表信息
                    row_dict = dict(zip(cols, row))
                    row_dict['作业来源'] = short_table_name
                    all_details.append(row_dict)
            
    except Exception as e:
        print(f"监控执行过程出错: {e}")
    finally:
        checker.close()
        
    # 生成 Markdown 报告
    today_str = get_date_str()
    report_lines = [
        f"### 📊 数据质量监控日报",
        f"> 📅 监控日期: {today_str}",
        ""
    ]
    
    for item in results:
        table = item['table']
        is_healthy = item['is_healthy']
        checks = item['checks']
        
        if is_healthy:
            # 正常显示
            # 格式：> **表名**
            #       > - 检查项1: 结果
            report_lines.append(f"> **{table}**")
            check_strs = []
            for c in checks:
                check_strs.append(f"{c['name']}: {c['msg']}")
            # 用 | 分隔显示在同一行，或者分行
            # 用户要求"罗列出来"，分行可能更清晰，但会太长。尝试一行显示。
            report_lines.append(f"> <font color=\"info\">{' | '.join(check_strs)}</font>")
            report_lines.append("") # 空行
        else:
            # 异常显示：更加明显
            # 使用一级或二级标题强调，或者加粗红色
            report_lines.append(f"### ❌ {table} (异常)")
            for c in checks:
                icon = "✅" if c['passed'] else "🔻"
                color = "info" if c['passed'] else "warning"
                report_lines.append(f"- {icon} {c['name']}: <font color=\"{color}\">{c['msg']}</font>")
            report_lines.append("") # 空行

    markdown_content = "\n".join(report_lines)
    
    # 初始化发送器
    sender = WeChatSender(WEBHOOK_URL)
    
    # 1. 发送 Markdown 消息
    print("正在发送企业微信 Markdown 通知...")
    response_md = sender.send_markdown(markdown_content)
    print(f"Markdown 发送结果: {response_md}")
    
    # 2. 生成并发送 CSV 明细文件
    # 保存到 reports 目录
    report_dir = os.path.join(os.getcwd(), "reports")
    csv_filename = os.path.join(report_dir, f"detail_report_{today_str}.csv")
    
    if save_details_to_csv(all_details, csv_filename):
        print(f"正在上传并发送文件: {csv_filename}...")
        media_id = sender.upload_file(csv_filename)
        if media_id:
            response_file = sender.send_file(media_id)
            print(f"文件发送结果: {response_file}")
        else:
            print("文件上传失败，跳过文件发送")

if __name__ == "__main__":
    run_monitor()
