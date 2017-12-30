# -*- coding: utf-8 -*-

from flask import Flask
from zatt.client import DistributedDict

import master_api

app = Flask(__name__)
app.register_blueprint(master_api.bp, url_prefox='/api')

d = DistributedDict('127.0.0.1', 5253)

if 'fid' not in d:
    d['fid'] = 0
    d['fdict'] = dict()
if 'vid' not in d:
    d['vid'] = 0
    d['vdict'] = dict()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
