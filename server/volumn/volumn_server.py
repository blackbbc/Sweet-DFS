# -*- coding: utf-8 -*-

import uuid
import pickle
import os
import time
import logging
import sys

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

from volumn import Volumn

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

config = dict()

if os.path.isfile('config'):
    config = pickle.load(open('config', 'rb'))
else:
    config['id'] = uuid.uuid4().hex
    pickle.dump(config, open('config', 'wb'))

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

while True:
    try:
        zk.create('/volumn/%s' % config['id'], b'localhost:8000', ephemeral=True, makepath=True)
        logger.info('Registered in zookeeper')
        break
    except NodeExistsError as e:
        logger.warn('Node exists. Retry after 1s...')
        time.sleep(1)

def main():
    volumn = Volumn(logger, 'localhost', 8000)
    volumn.start()

if __name__ == '__main__':
    main()
