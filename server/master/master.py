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

        self.vdb = dict()

        if os.path.isfile('vdb'):
            self.vdb = pickle.load(open('vdb', 'rb'))

        self.act_vol_serv = dict()
        self.act_vol_proxy = dict()

        self.serv = SimpleXMLRPCServer(
            (self.host, self.port),
            logRequests=True)

        for name in self._rpc_methods:
            self.serv.register_function(getattr(self, name))

    def _update_vdb(self):
        pickle.dump(self.vdb, open('vdb', 'wb'))

    def update_volumn(self, volumns):
        self.act_vol_serv = dict()
        self.act_vol_proxy = dict()
        for volumn in volumns:
            self.act_vol_serv[volumn[0]] = volumn[1]
            self.act_vol_proxy[volumn[0]] = ServerProxy(volumn[1])

    def assign_volumn(self, size):
        # TODO: 分配vid

        vid = 1

        vids = random.sample(self.act_vol_serv.keys(), 2)
        self.vdb[vid] = vids

        self._update_vdb()

        for vvid in vids:
            self.act_vol_proxy[vvid].assign_volumn(vid, size)

        return True

    def assign_fid(self):
        fid = '1,1'
        return fid

    def find_volumn(self, vid):
        vids = self.vdb[vid]
        addrs = []

        for vid in vids:
            if vid in self.act_vol_serv:
                addrs.append(self.act_vol_serv[vid])

        return addrs

    def start(self):
        self.logger.info('Start serving at %s:%d' % (self.host, self.port))
        self.serv.serve_forever()
