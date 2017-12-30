# -*- coding: utf-8 -*-

import sys
import time
import socket
import logging

from kazoo.client import KazooClient

from master import Master

# 获取logger实例，如果参数为空则返回root logger
logger = logging.getLogger()

# 指定logger输出格式
formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')

# 文件日志
file_handler = logging.FileHandler("volumn.log")
file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式

# 控制台日志
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter  # 也可以直接给formatter赋值

# 为logger添加的日志处理器
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 指定日志的最低输出级别，默认为WARN级别
logger.setLevel(logging.INFO)

zk = KazooClient(hosts='127.0.0.1:2181')

def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port

port = get_free_tcp_port()
master = Master(logger, 'localhost', port)

@zk.ChildrenWatch('/volumn')
def on_volumn_change(children):
    event = []
    for child in children:
        data, _ = zk.get('/volumn/%s' % child)
        event.append((child, data.decode()))
    master.update_volumn(event)

def main():
    zk.start()

    master.start()

if __name__ == '__main__':
    main()
