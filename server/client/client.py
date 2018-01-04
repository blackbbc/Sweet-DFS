# -*- coding:utf-8 -*-

import os
import io
import sys
import time
import json
import ntpath
import pickle
import socket
import random
import logging

from xmlrpc.client import ServerProxy

import humanfriendly

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

from werkzeug.utils import secure_filename

# 获取logger实例，如果参数为空则返回root logger
logger = logging.getLogger()

# 指定logger输出格式
formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')

# 文件日志
file_handler = logging.FileHandler("client.log")
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
zk.start()

UPLOAD_FOLDER = 'uploads'

act_master_proxy = dict()

fdb = dict()
if os.path.isfile('fdb'):
    fdb = pickle.load(open('fdb', 'rb'))

def update_db():
    pickle.dump(fdb, open('fdb', 'wb'))

@zk.ChildrenWatch('/master')
def on_master_change(children):
    act_master_proxy.clear()
    for child in children:
        data, _ = zk.get('/master/%s' % child)
        master = data.decode()
        act_master_proxy[master] = ServerProxy(master)

def get_master():
    return random.choice(list(act_master_proxy.values()))

def ls():
    for fdoc in fdb.values():
        print(fdoc['filename'], fdoc['fid'], humanfriendly.format_size(fdoc['size']))

CHUNK_SIZE = 64 * 1024 * 1024

def upload(path):

    if not os.path.isfile(path):
        print('%s not exist' % path)
        return

    master = get_master()

    filename = ntpath.basename(path)
    size = os.path.getsize(path)

    if size <= CHUNK_SIZE:
        with open(path, 'rb') as file:
            data = file.read()

        fid = master.assign_fid()
        vid, fkey = fid.split(',')
        volumns = master.find_volumn(int(vid))
        volumn = ServerProxy(random.choice(volumns))

        volumn.store(fid, data)

        fdoc = {'filename': filename, 'fid': fid, 'size': size, 'chunk': True}
        fdb[filename] = fdoc
        update_db()

        print(fid)
    else:
        usize = size
        fids = []
        with open(path, 'rb') as file:
            while usize > 0:
                data = file.read(CHUNK_SIZE)
                csize = len(data)

                fid = master.assign_fid()
                vid, fkey = fid.split(',')
                volumns = master.find_volumn(int(vid))
                volumn = ServerProxy(random.choice(volumns))

                volumn.store(fid, data)

                fids.append(fid)

                usize -= CHUNK_SIZE

        data = json.dumps(fids).encode()

        fid = master.assign_fid()
        vid, fkey = fid.split(',')
        volumns = master.find_volumn(int(vid))
        volumn = ServerProxy(random.choice(volumns))

        volumn.store(fid, data)

        fdoc = {'filename': filename, 'fid': fid, 'size': size, 'chunk': False}
        fdb[filename] = fdoc
        update_db()

        print(fid)

def download(filename):
    if filename not in fdb:
        print('%s not exist' % filename)
        return

    fdoc = fdb[filename]

    fid = fdoc['fid']
    size = fdoc['size']
    chunk = fdoc['chunk']

    master = get_master()

    vid, fkey = fid.split(',')

    volumns = master.find_volumn(int(vid))

    volumn = ServerProxy(random.choice(volumns))

    data = volumn.download(fid).data

    if chunk:
        with open(filename, 'wb') as file:
            file.write(data)

        print('Download success')
    else:
        fids = json.loads(data)

        with open(filename, 'wb') as file:
            for fid in fids:
                data = volumn.download(fid).data
                file.write(data)

        print('Download success')

def assign(size):
    size = int(size)
    master = get_master()

    vid = master.assign_volumn(int(size))

    print(vid)

def delete():
    pass

def status():
    pass

def main():
    while True:
        cmd = input(">> ").split()
        if not cmd:
            continue
        elif cmd[0] == 'ls':
            ls()
        elif cmd[0] == 'upload':
            upload(cmd[1])
        elif cmd[0] == 'download':
            download(cmd[1])
        elif cmd[0] == 'assign':
            assign(cmd[1])
        elif cmd[0] == 'status':
            status()

if __name__ == '__main__':
    main()
