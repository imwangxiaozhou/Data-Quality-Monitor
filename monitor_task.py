import datetime
import csv
import os
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

def check_table_status(max_ds, count):
    """
    检查表状态 (基础检查: 日期和数据量)
    :param max_ds: 最大分区日期
    :param count: 数据量
    :return: (is_healthy, message)
    """
    today = get_date_str(0)
    yesterday = get_date_str(1)
    
    # 校验规则 1: max_ds 必须是今天或昨天
    is_date_valid = max_ds in [today, yesterday]
    
    # 校验规则 2: 数据量必须 > 0
    is_count_valid = count > 0
    
    is_healthy = is_date_valid and is_count_valid
    
    if not is_date_valid:
        return False, "日期滞后"
    if not is_count_valid:
        return False, "数据量为0"
        
    return True, "正常"

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
            
            # 1. 基础检查
            is_healthy, msg = check_table_status(max_ds, count)
            
            # 2. status 字段分布检查 (仅当基础检查通过且有数据时)
            status_msg = ""
            if is_healthy and count > 0:
                has_status, is_abnormal, dist_msg = checker.check_status_distribution(table, max_ds)
                if has_status and is_abnormal:
                    is_healthy = False
                    status_msg = f" ({dist_msg})"
                elif has_status:
                    status_msg = "" # 正常不需要额外显示
            
            results.append({
                "table": short_table_name,
                "ds": max_ds,
                "count": count,
                "is_healthy": is_healthy,
                "msg": msg + status_msg
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
        "", # 空行
        "**监控明细:**"
    ]
    
    has_error = False
    
    for item in results:
        table = item['table']
        ds = item['ds'] if item['ds'] else "NULL"
        count = item['count']
        is_healthy = item['is_healthy']
        msg = item.get('msg', '')
        
        # 格式化每行输出
        if is_healthy:
            # 正常：绿色
            line = f"- {table}: <font color=\"info\">{ds} (数据量: {count})</font>"
        else:
            # 异常：红色警示
            has_error = True
            # 如果是 status 异常，显示具体信息
            line = f"- {table}: <font color=\"warning\">⚠️ {ds} (数据量: {count}) {msg}</font>"
            
        report_lines.append(line)

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
