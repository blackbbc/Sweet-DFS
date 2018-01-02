# -*- coding: utf-8 -*-

import os
import pickle
import shutil
import random

from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
from pysyncobj import SyncObj, SyncObjConf, replicated
from pysyncobj.batteries import ReplCounter, ReplDict

import config

class Master(SyncObj):

    _rpc_methods = ['assign_volumn', 'assign_fid', 'find_volumn']

    def __init__(self, logger, host, port):
        cfg = SyncObjConf()
        cfg.fullDumpFile = 'raft.bin'
        cfg.logCompactionMinTime = 10
        cfg.useFork = True

        self.serv = SimpleXMLRPCServer(
            (host, port),
            logRequests=True)

        for name in self._rpc_methods:
            self.serv.register_function(getattr(self, name))

        self.logger = logger
        self.host = host
        self.port = port

        self.act_vol_serv = dict()
        self.act_vol_proxy = dict()

        self.vid = ReplCounter()
        self.fkey = ReplCounter()
        self.db = ReplDict()
        super(Master, self).__init__(config.addr, config.clusters, cfg, consumers=[self.vid, self.fkey, self.db])

    def update_master(self, masters):
        pass

    def update_volumn(self, volumns):
        self.act_vol_serv.clear()
        self.act_vol_proxy.clear()
        for volumn in volumns:
            self.act_vol_serv[volumn[0]] = volumn[1]
            self.act_vol_proxy[volumn[0]] = ServerProxy(volumn[1])

    def assign_volumn(self, size):
        vid = self.vid.inc(sync=True)

        vids = random.sample(self.act_vol_serv.keys(), 2)

        for vvid in vids:
            self.act_vol_proxy[vvid].assign_volumn(vid, size)

        self.db.set(vid, vids)

        return vid

    def assign_fid(self):
        vid = random.choice(list(self.db.keys()))
        fkey = self.inc_fkey(sync=True)

        fid = '%d,%d' % (vid, fkey)
        return fid

    def find_volumn(self, vid):
        vids = self.db[vid]
        addrs = []

        for vid in vids:
            if vid in self.act_vol_serv:
                addrs.append(self.act_vol_serv[vid])

        return addrs

    def start(self):
        self.logger.info('Start serving at %s:%d' % (self.host, self.port))
        self.serv.serve_forever()
