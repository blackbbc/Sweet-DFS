# -*- coding: utf-8

import os
import sys
sys.path.insert(0, '../util')
import traceback
import random
import pickle
import shutil

from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

from rwlock import RWLock
from threadxmlrpc import ThreadXMLRPCServer

class Volumn(object):

    _rpc_methods = ['assign_volumn', 'store', 'replica', 'download', 'status', 'balance',
            'migrate_volumn_to', 'migrate_volumn_from', 'delete_file', 'delete_volumn']

    def __init__(self, logger, host, port):
        self.logger = logger
        self.host = host
        self.port = port

        self.lock = RWLock()

        self.vdb = dict()
        self.fdb = dict()
        if os.path.isfile('vdb'):
            self.vdb = pickle.load(open('vdb', 'rb'))
        if os.path.isfile('fdb'):
            self.fdb = pickle.load(open('fdb', 'rb'))

        self.act_mst_serv = list()

        self.serv = ThreadXMLRPCServer(
            (self.host, self.port),
            logRequests=True)

        for name in self._rpc_methods:
            self.serv.register_function(getattr(self, name))

    def _update_vdb(self):
        pickle.dump(self.vdb, open('vdb', 'wb'))

    def _update_fdb(self):
        pickle.dump(self.fdb, open('fdb', 'wb'))

    def update_master(self, masters):
        self.act_mst_serv = masters

    def get_master(self):
        return ServerProxy(random.choice(self.act_mst_serv))

    def assign_volumn(self, vid, size):
        path = 'data/%s' % vid

        if not os.path.isdir('data'):
            os.mkdir('data')

        try:
            self.lock.acquire_read()

            with open(path, 'wb') as f:
                f.seek(size - 1)
                f.write(b'\0')

            vdoc = dict()
            vdoc['vid'] = vid
            vdoc['path'] = path
            vdoc['size'] = size
            vdoc['counter'] = 0

            self.vdb[vid] = vdoc
            self._update_vdb()

            return True
        except:
            return False
        finally:
            self.lock.release()

    def migrate_volumn_to(self, vid, to_addr):
        try:
            vdoc = self.vdb[vid]
            path = vdoc['path']

            s = ServerProxy(to_addr)

            with open(path, 'rb') as f:
                while True:
                    data = f.read(64*1024*1024)
                    if data:
                        s.migrate_volumn_from(vid, data, vdoc)
                        self.logger.info('Send data...')
                    else:
                        fdocs = {k: v for k, v in self.fdb.items() if k.startswith('%d,' % vid)}
                        self.logger.info('Send metadata')
                        s.migrate_volumn_from(vid, data, vdoc, fdocs, True)
                        break

            return True
        except:
            return False

    def migrate_volumn_from(self, vid, data, vdoc, fdocs=None, done=False):
        path = vdoc['path']

        if not os.path.isdir('data'):
            os.mkdir('data')

        if done:
            self.vdb[vid] = vdoc
            self._update_vdb()
            self.fdb = {**self.fdb, **fdocs}
            self._update_fdb()
            self.logger.info('Build replica %d success' % vid)
        else:
            with open(path, 'ab') as f:
                f.write(data.data)

        return True

    def store(self, fid, data):
        vid, _ = fid.split(',')
        vid = int(vid)

        try:
            self.replica(fid, data)
            master = self.get_master()
            volumns = master.find_writable_volumn(vid)

            if not volumns:
                return False

            for volumn in volumns:
                if volumn != 'http://%s:%d' % (self.host, self.port):
                    s = ServerProxy(volumn)
                    s.replica(fid, data)

            return True
        except Exception as e:
            self.logger.exception('Got an exception')
            return False

    def replica(self, fid, data):
        data = data.data
        vid, _ = fid.split(',')
        vid = int(vid)

        self.lock.acquire_write()

        vdoc = self.vdb[vid]
        path = vdoc['path']
        offset = vdoc['counter']

        size = len(data)
        vdoc['counter'] += size

        self.lock.release()

        with open(path, 'r+b') as f:
            f.seek(offset)
            f.write(data)

        fdoc = dict()
        fdoc['fid'] = fid
        fdoc['offset'] = offset
        fdoc['size'] = size
        fdoc['delete'] = False

        self.vdb[vid] = vdoc
        self._update_vdb()

        self.fdb[fid] = fdoc
        self._update_fdb()

        return True

    def update_file(self, fid, data):
        pass

    def delete_file(self, fid, sync=True):
        vid, _ = fid.split(',')
        vid = int(vid)

        try:
            if sync:
                master = self.get_master()
                volumns = master.find_writable_volumn(vid)

                if not volumns:
                    return False

                for volumn in volumns:
                    if volumn != 'http://%s:%d' % (self.host, self.port):
                        s = ServerProxy(volumn)
                        s.delete_file(fid, False)

            fdoc = self.fdb[fid]
            fdoc['delete'] = True

            self._update_fdb()

            return True
        except Exception as e:
            self.logger.exception('Got an exception')
            return False


    def delete_volumn(self, vid):
        pass

    def download(self, fid):
        vid, _ = fid.split(',')
        vid = int(vid)

        if vid not in self.vdb or fid not in self.fdb:
            return None

        try:
            self.lock.acquire_read()

            vdoc = self.vdb[vid]
            fdoc = self.fdb[fid]

            if fdoc['delete'] == True:
                return None

            path = vdoc['path']
            offset = fdoc['offset']
            size = fdoc['size']

            with open(path, 'rb') as f:
                f.seek(offset)
                data = f.read(size)

            return data
        except:
            return None
        finally:
            self.lock.release()

    def balance(self, vid):
        try:
            self.lock.acquire_write()

            vdoc = self.vdb[vid]
            fdocs = self.fdb

            tfdocs = fdocs.copy()

            tvdoc = vdoc.copy()
            tvdoc['counter'] = 0

            path = vdoc['path']
            size = vdoc['size']

            with open(path + '.tmp', 'wb') as f:
                f.seek(size - 1)
                f.write(b'\0')

            with open(path, 'r+b') as from_file, open(path + '.tmp', 'r+b') as to_file:
                to_file.seek(0)
                for fdoc in fdocs.values():
                    if fdoc['fid'].startswith('%d,' % vid) and fdoc['delete'] == False:
                        from_file.seek(fdoc['size'])
                        data = from_file.read(fdoc['size'])
                        to_file.write(data)

                        tfdoc = fdoc.copy()
                        tfdoc['offset'] = tvdoc['counter']
                        tvdoc['counter'] += fdoc['size']
                        tfdocs[fdoc['fid']] = tfdoc

            os.remove(path)
            os.rename(path + '.tmp', path)

            self.vdb[vid] = tvdoc
            self.fdocs = tfdocs

            return True
        except:
            self.logger.exception('Got an exception')
            return False
        finally:
            self.lock.release()

    def status(self):
        status = dict()
        total, used, free = shutil.disk_usage(__file__)
        status['total'] = str(total)
        status['used'] = str(used)
        status['free'] = str(free)
        status['vdb'] = {str(vid):vdoc for vid, vdoc in self.vdb.items()}
        return status

    def start(self):
        self.logger.info('Start serving at %s:%d' % (self.host, self.port))
        self.serv.serve_forever()
