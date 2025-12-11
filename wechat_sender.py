import requests
import json
import os
from typing import List, Optional

class WeChatSender:
    def __init__(self, webhook_url: str):
        """
        初始化企业微信发送器
        :param webhook_url: 企业微信群机器人的 Webhook 地址
        """
        self.webhook_url = webhook_url
        self.headers = {'Content-Type': 'application/json'}

    def _send(self, data: dict) -> dict:
        """
        发送请求到企业微信
        :param data: 发送的数据字典
        :return: 响应结果
        """
        try:
            response = requests.post(
                self.webhook_url,
                headers=self.headers,
                data=json.dumps(data)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"发送消息失败: {e}")
            return {"errcode": -1, "errmsg": str(e)}

    def upload_file(self, file_path: str) -> Optional[str]:
        """
        上传文件到企业微信
        :param file_path: 文件绝对路径
        :return: media_id 或 None
        """
        if not os.path.exists(file_path):
            print(f"文件不存在: {file_path}")
            return None
            
        # 构造上传 URL: 将 webhook_url 中的 send 替换为 upload_media，并追加 type=file
        # 假设 webhook_url 格式为 .../webhook/send?key=...
        upload_url = self.webhook_url.replace("webhook/send", "webhook/upload_media") + "&type=file"
        
        try:
            with open(file_path, 'rb') as f:
                files = {'media': f}
                # 注意：上传文件不能带 Content-Type: application/json 头，requests 会自动处理 multipart/form-data
                response = requests.post(upload_url, files=files)
                response.raise_for_status()
                result = response.json()
                
                if result.get("errcode") == 0:
                    return result.get("media_id")
                else:
                    print(f"上传文件失败: {result}")
                    return None
        except Exception as e:
            print(f"上传文件异常: {e}")
            return None

    def send_file(self, media_id: str) -> dict:
        """
        发送文件消息
        :param media_id: 上传文件获取的 media_id
        :return: 响应结果
        """
        data = {
            "msgtype": "file",
            "file": {
                "media_id": media_id
            }
        }
        return self._send(data)

    def send_text(self, content: str, mentioned_list: Optional[List[str]] = None, mentioned_mobile_list: Optional[List[str]] = None) -> dict:
        """
        发送文本消息
        :param content: 文本内容
        :param mentioned_list: userid的列表，提醒群中的指定成员(@某个成员)，@all表示提醒所有人
        :param mentioned_mobile_list: 手机号列表，提醒手机号对应的群成员(@某个成员)，@all表示提醒所有人
        :return: 响应结果
        """
        data = {
            "msgtype": "text",
            "text": {
                "content": content
            }
        }
        if mentioned_list:
            data["text"]["mentioned_list"] = mentioned_list
        if mentioned_mobile_list:
            data["text"]["mentioned_mobile_list"] = mentioned_mobile_list
            
        return self._send(data)

    def send_markdown(self, content: str) -> dict:
        """
        发送 Markdown 消息
        :param content: markdown内容
        :return: 响应结果
        """
        data = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
        return self._send(data)

if __name__ == "__main__":
    # 使用示例
    # 请替换为实际的 webhook url
    webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=d741ee77-b177-4f92-b478-e5357cadf990"
    
    sender = WeChatSender(webhook_url)
    
    # 发送文本消息示例
    sender.send_text("Hello World! 这是一个测试消息。")
    
    # 发送 Markdown 消息示例
    # markdown_content = """# 标题一
    # ## 标题二
    # **加粗内容**
    # [链接](https://work.weixin.qq.com/api/doc)
    # <font color="info">绿色字体</font>
    # """
    # sender.send_markdown(markdown_content)
