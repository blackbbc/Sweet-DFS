# -*- coding: utf-8 -*-

import os
import sys
sys.path.insert(0, '../util')
import time
import pickle
import shutil
import random
import _thread

from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
from pysyncobj import SyncObj, SyncObjConf, replicated
from pysyncobj.batteries import ReplCounter, ReplDict

from rwlock import RWLock
from threadxmlrpc import ThreadXMLRPCServer

import config

class Master(SyncObj):

    _rpc_methods = ['assign_volumn', 'assign_fid', 'find_volumn', 'status']

    def __init__(self, logger, host, port):
        cfg = SyncObjConf()
        cfg.fullDumpFile = 'raft.bin'
        cfg.logCompactionMinTime = 10
        cfg.useFork = True

        self.serv = ThreadXMLRPCServer(
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

    def _recover(self, vid, dead_vid, from_vid, from_proxy, to_vid, to_addr):
        self.logger.info('Migrate volumn from %s to %s after 10 minutes' % (from_vid, to_vid))

        t = 60

        while t > 0:
            time.sleep(1)
            if dead_vid in self.act_vol_serv.keys():
                self.logger.info('Volumn %s becomes live. Stop recover' % dead_vid)
                _thread.exit()
            t -= 1

        self.logger.info('Begin to migrate volumn from %s to %s...!' % (from_vid, to_vid))
        from_proxy.migrate_volumn_to(vid, to_addr)
        self.db[vid].remove(from_vid)
        self.db[vid].append(to_vid)
        self.logger.info('Migrate volumn from %s to %s succeed!' % (from_vid, to_vid))

    # 检查volumn下线的情况，搬运
    def update_volumn(self, volumns):
        if self._isLeader():
            old_volumns = set(self.act_vol_serv.keys())
            new_volumns = set([volumn[0] for volumn in volumns])

            off_volumns = list(old_volumns - new_volumns)

            if off_volumns:
                self.logger.info('{} volumns become offline'.format(off_volumns))

            for off_volumn in off_volumns:
                for vid, vvids in self.db.items():
                    if off_volumn in vvids:
                        for recov_vid in vvids:
                            if recov_vid != off_volumn:
                                from_vid = recov_vid
                                from_proxy = self.act_vol_proxy[recov_vid]
                                to_vid = random.choice(list(set(self.act_vol_serv.keys()) - set(vvids)))
                                to_addr = self.act_vol_serv[to_vid]

                                # 开一个线程去传
                                _thread.start_new_thread(self._recover, (vid, off_volumn, from_vid, from_proxy, to_vid, to_addr))

                                break


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
        fkey = self.fkey.inc(sync=True)

        fid = '%d,%d' % (vid, fkey)
        return fid

    def find_volumn(self, vid):
        vids = self.db[vid]
        addrs = []

        for vid in vids:
            if vid in self.act_vol_serv:
                addrs.append(self.act_vol_serv[vid])

        return addrs

    def status(self):
        status = dict()

        vol_status = dict()
        for vol_serv, vol_serv_proxy in self.act_vol_proxy:
            vv = vol_serv_proxy.status()
            vol_status[vol_serv] = vv

        for vid, vvids in self.db:
            status[vid] = vvids

        return vid

    def start(self):
        self.logger.info('Start serving at %s:%d' % (self.host, self.port))
        self.serv.serve_forever()
