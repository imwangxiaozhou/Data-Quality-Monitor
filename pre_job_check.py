import sys
import time
from hive_checker import HiveChecker
from wechat_sender import WeChatSender

# 配置信息
HIVE_HOST = '192.168.10.3'
HIVE_PORT = 10000
HIVE_USER = 'hadoop'
WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=d741ee77-b177-4f92-b478-e5357cadf990"

def main():
    if len(sys.argv) < 3:
        print("Usage: python pre_job_check.py <table_name> <target_date> [max_retries]")
        print("Example: python pre_job_check.py glsx_data_warehouse.ads_test 2025-12-11 4")
        sys.exit(1)

    table_name = sys.argv[1]
    target_date = sys.argv[2] # 期望格式 YYYY-MM-DD
    
    # 默认重试 4 次，如果有第3个参数则使用该参数
    max_retries = 4
    if len(sys.argv) >= 4:
        try:
            max_retries = int(sys.argv[3])
        except ValueError:
            print(f"警告: 传入的重试次数 '{sys.argv[3]}' 无效，将使用默认值 {max_retries}")

    print(f"=== 开始前置任务检查 ===")
    print(f"目标表: {table_name}")
    print(f"目标日期: {target_date}")
    print(f"最大重试次数: {max_retries}")
    
    checker = HiveChecker(HIVE_HOST, HIVE_PORT, HIVE_USER)
    sender = WeChatSender(WEBHOOK_URL)
    
    # 策略: 跑 max_retries 次，每次间隔5分钟 (300秒)
    # 第1次立即执行，失败则等待5分钟执行第2次...
    retry_interval = 300  
    
    for i in range(max_retries):
        attempt = i + 1
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"\n[{timestamp}] 第 {attempt}/{max_retries} 次检查...")
        
        try:
            # 获取最大日期值 (ds 或 createtime)
            max_val = checker.get_max_date_value(table_name)
            
            if max_val:
                # 格式化处理: 截取空格前的日期部分
                # 兼容 '2025-12-11 10:00:00' 和 '2025-12-11'
                current_date_str = str(max_val).strip().split(' ')[0]
                print(f"当前数据库最大日期: {current_date_str}")
                
                if current_date_str == target_date:
                    msg = (
                        f"✅ **前置任务检查通过**\n"
                        f"> 表名: `{table_name}`\n"
                        f"> 目标日期: {target_date}\n"
                        f"> 检查结果: {table_name} 作为前置已经完成了"
                    )
                    print("检查通过，发送通知...")
                    sender.send_markdown(msg)
                    sys.exit(0)
                else:
                    print(f"日期不匹配 ({current_date_str} != {target_date})")
            else:
                print("未查询到有效日期值 (可能表为空或无指定字段)")
                
        except Exception as e:
            print(f"检查过程发生异常: {e}")
            
        # 如果不是最后一次，则等待
        if attempt < max_retries:
            print(f"等待 {retry_interval} 秒后重试...")
            time.sleep(retry_interval)
            
    # 循环结束仍未通过
    error_msg = (
        f"❌ **前置任务未完成 (异常报警)**\n"
        f"> 表名: `{table_name}`\n"
        f"> 目标日期: {target_date}\n"
        f"> 状态: 已重试 {max_retries} 次仍不满足条件"
    )
    print("\n检查未通过，发送报警通知...")
    sender.send_markdown(error_msg)
    sys.exit(1)

if __name__ == "__main__":
    main()
