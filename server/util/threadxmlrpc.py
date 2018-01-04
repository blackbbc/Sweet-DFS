from xmlrpc.server import SimpleXMLRPCServer
from socketserver import ThreadingMixIn

class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass
