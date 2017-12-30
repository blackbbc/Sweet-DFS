# -*- coding: utf-8

import os
import pickle

from xmlrpc.server import SimpleXMLRPCServer

class Volumn(object):

    _rpc_methods = ['assign_volumn', 'store', 'download', 'status']

    def __init__(self, logger, host, port):
        self.logger = logger
        self.host = host
        self.port = port

        self.vdb = dict()
        self.fdb = dict()
        if os.path.isfile('vdb'):
            self.vdb = pickle.load(open('vdb', 'rb'))
        if os.path.isfile('fdb'):
            self.fdb = pickle.load(open('fdb', 'rb'))

        self.serv = SimpleXMLRPCServer(
            (self.host, self.port),
            logRequests=True)

        for name in self._rpc_methods:
            self.serv.register_function(getattr(self, name))

    def _update_vdb(self):
        pickle.dump(self.vdb, open('vdb', 'wb'))

    def _update_fdb(self):
        pickle.dump(self.fdb, open('fdb', 'wb'))

    def assign_volumn(self, vid, size):
        path = 'data/%s' % vid

        if not os.path.isdir('data'):
            os.mkdir('data')

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

    def store(self, fid, data):
        data = data.data
        vid, fkey = fid.split(',')
        vid = int(vid)
        fkey = int(fkey)

        vdoc = self.vdb[vid]
        path = vdoc['path']
        offset = vdoc['counter']
        size = len(data)
        vdoc['counter'] += size
        self.vdb.update()

        with open(path, 'r+b') as f:
            f.seek(offset)
            f.write(data)

        fdoc = dict()
        fdoc['fkey'] = fkey
        fdoc['offset'] = offset
        fdoc['size'] = size

        self.fdb[fkey] = fdoc
        self._update_fdb()

        return True

    def download(self, fid):
        vid, fkey = fid.split(',')
        vid = int(vid)
        fkey = int(fkey)

        vdoc = self.vdb[vid]
        fdoc = self.fdb[fkey]
        path = vdoc['path']
        offset = fdoc['offset']
        size = fdoc['size']

        with open(path, 'rb') as f:
            f.seek(offset)
            data = f.read(size)

        return data

    def status(self):
        return self.vdb

    def start(self):
        self.logger.info('Start serving at %s:%d' % (self.host, self.port))
        self.serv.serve_forever()
