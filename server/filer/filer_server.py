# -*- coding:utf-8 -*-

import os
import io
import sys
import time
import pickle
import socket
import random
import logging

from xmlrpc.client import ServerProxy

import humanfriendly

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

from flask import Flask, request, render_template, send_file, jsonify
from werkzeug.utils import secure_filename

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
zk.start()

UPLOAD_FOLDER = 'uploads'

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

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

@app.template_filter()
def format_size(size):
    return humanfriendly.format_size(size)

@app.route('/')
def index():
    return render_template('index.html', files=fdb)

@app.route('/assign/volumn', methods=['POST', 'GET'])
def assign_volumn():
    size = request.form['size']
    master = get_master()

    vid = master.assign_volumn(int(size))

    return jsonify({'vid': vid})

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'GET':
        return render_template('upload.html')
    else:
        master = get_master()
        file = request.files['file']
        filename = secure_filename(file.filename)

        data = file.read()
        size = len(data)

        fid = master.assign_fid()
        vid, fkey = fid.split(',')
        volumns = master.find_volumn(int(vid))

        volumn = ServerProxy(random.choice(volumns))

        volumn.store(fid, data)

        fdoc = {'filename': filename, 'fid': fid, 'size': size}
        fdb[fid] = fdoc
        update_db()

        return fid

@app.route('/download/<string:fid>')
def download(fid):
    vid, fkey = fid.split(',')
    master = get_master()
    volumns = master.find_volumn(int(vid))

    volumn = ServerProxy(random.choice(volumns))

    fdoc = fdb[fid]

    filename = fdoc['filename']

    data = volumn.download(fid).data

    return send_file(io.BytesIO(data), attachment_filename=filename)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
