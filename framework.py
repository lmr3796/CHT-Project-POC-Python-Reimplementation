#! /usr/bin/env python

import socket
import xmlrpclib

import config


from SimpleXMLRPCServer import SimpleXMLRPCServer

# TODO: make those components registerable?
def get_dispatcher():
    addr = 'http://%s:%d'%('localhost', config.port['Dispatcher'])
    return xmlrpclib.ServerProxy(addr, allow_none=True)
    
def get_dicision_maker():
    return

def build_rpc_server_from_component(comp):
    server = SimpleXMLRPCServer(('', config.port[comp.__class__.__name__]), allow_none=True)
    server.register_instance(comp)
    return server 

''' For example only'''
class Example(object):
    def add(self, a, b):
        return a+b

    def long_add(self, a, b):
        t = 0
        for i in xrange(100000000):
            t += 1
        return self.add(a,b)

if __name__ == '__main__':
    server = SimpleXMLRPCServer(('', 3796), allow_none=True)
    server.register_instance(Example())
    server.serve_forever()
