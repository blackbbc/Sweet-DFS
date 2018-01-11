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
from pysyncobj.batteries import ReplCounter, ReplDict, ReplList

from rwlock import RWLock
from threadxmlrpc import ThreadXMLRPCServer

import config

class Master(SyncObj):

    _rpc_methods = ['assign_volumn', 'assign_fid', 'find_volumn', 'find_writable_volumn',
            'volumn_status', 'node_status']

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

        self.lock = RWLock()

        self.act_vol_serv = dict()
        self.writable_vid = ReplList() # 可写的vid

        self.vid = ReplCounter()
        self.fkey = ReplCounter()
        self.db = ReplDict()

        super(Master, self).__init__(config.addr, config.clusters, cfg, consumers=[self.vid, self.fkey, self.db, self.writable_vid])


    def update_master(self, masters):
        pass

    def _recover(self, vid, dead_vid, from_vid, to_vid):
        from_proxy = ServerProxy(self.act_vol_serv[from_vid])
        to_addr = self.act_vol_serv[to_vid]

        self.logger.info('Begin to migrate volumn %d from %s to %s...!' % (vid, from_vid, to_vid))
        from_proxy.migrate_volumn_to(vid, to_addr)
        self.logger.info('Migrate volumn %d from %s to %s succeed!' % (vid, from_vid, to_vid))
        vids = self.db[vid]
        vids.remove(dead_vid)
        vids.append(to_vid)
        self.db.set(vid, vids, sync=True)
        self.update_writable_volumn()
        self.logger.info('Remove %s, append %s' % (dead_vid, to_vid))

    def _check(self, dead_vid):
        self.logger.info('Monitor dead volumn server %s ...' % dead_vid)

        t = 60

        while t > 0:
            time.sleep(1)
            if dead_vid in self.act_vol_serv.keys():
                self.logger.info('Volumn %s becomes live. Stop recover' % dead_vid)
                _thread.exit()
            t -= 1

        for vid, vvids in self.db.items():
            if dead_vid in vvids:
                for recov_vid in vvids:
                    if recov_vid != dead_vid and recov_vid in self.act_vol_serv.keys():
                        from_vid = recov_vid
                        avl_vids = list(set(self.act_vol_serv.keys()) - set(vvids))
                        if avl_vids:
                            to_vid = random.choice(avl_vids)
                            _thread.start_new_thread(self._recover, (vid, dead_vid, from_vid, to_vid))
                        else:
                            self.logger.warn('No available volumns to migrate')
                        break

    def update_writable_volumn(self, checkLeader=True):
        if checkLeader and not self._isLeader():
            return

        writable_vid = list()

        for vid, vvids in self.db.items():
            flag = True
            for vvid in vvids:
                if vvid not in self.act_vol_serv.keys():
                    flag = False
                    break
            if flag:
                writable_vid.append(vid)

        self.writable_vid.reset(writable_vid, sync=True)


    # 检查volumn下线的情况，搬运
    def update_volumn(self, volumns):
        if self._isLeader():
            old_volumns = set(self.act_vol_serv.keys())
            new_volumns = set([volumn[0] for volumn in volumns])

            off_volumns = list(old_volumns - new_volumns)

            if off_volumns:
                self.logger.info('{} volumns become offline'.format(off_volumns))

            for off_volumn in off_volumns:
                _thread.start_new_thread(self._check, (off_volumn,))

        self.act_vol_serv.clear()
        for volumn in volumns:
            self.act_vol_serv[volumn[0]] = volumn[1]

        while not self._isReady():
            time.sleep(1)

        self.update_writable_volumn()

    def assign_volumn(self, size):
        vid = self.vid.inc(sync=True)

        vids = random.sample(self.act_vol_serv.keys(), 2)

        for vvid in vids:
            s = ServerProxy(self.act_vol_serv[vvid])
            s.assign_volumn(vid, size)

        self.db.set(vid, vids, sync=True)
        self.update_writable_volumn(False)

        return vid

    def assign_fid(self):
        if not self.writable_vid:
            return ''

        vid = random.choice(list(self.writable_vid))
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

    def find_writable_volumn(self, vid):
        if vid in self.writable_vid:
            return self.find_volumn(vid)
        else:
            return []

    def volumn_status(self):
        res = dict()

        vol_status = dict()

        for vol_serv_id, vol_serv in self.act_vol_serv.items():
            try:
                s = ServerProxy(vol_serv)
                vv = s.status()
                vol_status[vol_serv_id] = vv
            except:
                pass


        for vid, vvids in self.db.items():
            sdoc = dict()
            ava_nodes = list(set(vol_status.keys()) & set(vvids))
            sdoc['tat_node_num'] = len(vvids)
            sdoc['ava_node_num'] = len(ava_nodes)

            if ava_nodes:
                vol_sdoc = vol_status[ava_nodes[0]]
                vdoc = vol_sdoc['vdb'][str(vid)]
                sdoc['total_size'] = vdoc['size']
                sdoc['used_size'] = vdoc['counter']
                sdoc['free_size'] = sdoc['total_size'] - sdoc['used_size']
            else:
                sdoc['total_size'] = 0
                sdoc['used_size'] = 0
                sdoc['free_size'] = 0

            res[str(vid)] = sdoc

        return res

    def node_status(self):
        res = dict()

        for vol_serv_id, vol_serv in self.act_vol_serv.items():
            try:
                s = ServerProxy(vol_serv)
                vv = s.status()

                vol_status = dict()
                vol_status['addr'] = vol_serv
                vol_status['total'] = vv['total']
                vol_status['used'] = vv['used']
                vol_status['free'] = vv['free']
                vol_status['nodes'] = list(vv['vdb'].keys())

                for node in vol_status['nodes']:
                    vids = self.db.get(int(node), [])
                    if vol_serv_id not in vids:
                        vol_status['nodes'].remove(node)

                res[vol_serv_id] = vol_status
            except:
                self.logger.exception('Got an exception')

        return res


    def start(self):
        self.logger.info('Start serving at %s:%d' % (self.host, self.port))
        self.serv.serve_forever()
