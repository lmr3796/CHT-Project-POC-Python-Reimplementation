#! /usr/bin/env python

import os
import subprocess

import framework
import config

from SimpleXMLRPCServer import SimpleXMLRPCServer

class Worker(object):
    def run_job(self, cmd):
        result = subprocess.check_output(cmd)
        return result

if __name__ == '__main__':
    framework.build_rpc_server_from_component(Worker()).serve_forever()
