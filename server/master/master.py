# -*- coding: utf-8 -*-

import os
import pickle
import shutil
import random

from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

class Master(object):

    _rpc_methods = ['assign_volumn', 'assign_fid', 'find_volumn']

    def __init__(self, logger, host, port):
        self.logger = logger
        self.host = host
        self.port = port

        self.db = dict()

        if os.path.isfile('db'):
            self.db = pickle.load(open('db', 'rb'))

        if 'vdb' not in self.db:
            self.db['vdb'] = dict()
            self.db['vid'] = 0
            self.db['fkey'] = 0

        self.act_vol_serv = dict()
        self.act_vol_proxy = dict()

        self.serv = SimpleXMLRPCServer(
            (self.host, self.port),
            logRequests=True)

        for name in self._rpc_methods:
            self.serv.register_function(getattr(self, name))

    def _update_db(self):
        pickle.dump(self.db, open('db', 'wb'))

    def update_volumn(self, volumns):
        self.act_vol_serv.clear()
        self.act_vol_proxy.clear()
        for volumn in volumns:
            self.act_vol_serv[volumn[0]] = volumn[1]
            self.act_vol_proxy[volumn[0]] = ServerProxy(volumn[1])

    def assign_volumn(self, size):
        vid = self.db['vid']
        self.db['vid'] += 1

        vids = random.sample(self.act_vol_serv.keys(), 2)
        self.db['vdb'][vid] = vids

        self._update_db()

        for vvid in vids:
            self.act_vol_proxy[vvid].assign_volumn(vid, size)

        return True

    def assign_fid(self):
        vid = random.choice(list(self.db['vdb'].keys()))
        fkey = self.db['fkey']
        self.db['fkey'] += 1

        self._update_db()

        fid = '%d,%d' % (vid, fkey)
        return fid

    def find_volumn(self, vid):
        vids = self.db['vdb'][vid]
        addrs = []

        for vid in vids:
            if vid in self.act_vol_serv:
                addrs.append(self.act_vol_serv[vid])

        return addrs

    def start(self):
        self.logger.info('Start serving at %s:%d' % (self.host, self.port))
        self.serv.serve_forever()
