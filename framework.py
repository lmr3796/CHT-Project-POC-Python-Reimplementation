#! /usr/bin/env python

import config
from SimpleXMLRPCServer import SimpleXMLRPCServer

class Example(object):
    def add(self, a, b):
        return a+b

    def long_add(self, a, b):
        t = 0
        for i in range(100000000):
            t += 1
        return self.add(a,b)

def build_rpc_server_from_component(comp):
    server = SimpleXMLRPCServer(('', config.port[comp.__class__.__name__]))
    server.register_instance(comp)
    return server 

if __name__ == '__main__':
    server = SimpleXMLRPCServer(('', 3796))
    server.register_instance(Example())
    server.serve_forever()
