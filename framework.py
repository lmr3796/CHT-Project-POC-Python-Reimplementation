#! /usr/bin/env python

import socket
import xmlrpclib

import config


from SimpleXMLRPCServer import SimpleXMLRPCServer

def run_jobs(job_set):
    pass

def build_rpc_server_from_component(comp):
    socket.setdefaulttimeout(30)
    server = SimpleXMLRPCServer(('', config.port[comp.__class__.__name__]))
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
    server = SimpleXMLRPCServer(('', 3796))
    server.register_instance(Example())
    server.serve_forever()
