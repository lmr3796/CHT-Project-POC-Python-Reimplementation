#! /usr/bin/env python

import config
from SimpleXMLRPCServer import SimpleXMLRPCServer

class Worker(object):
    def run_job(self, job):
        job.run()
        return job.get_result()

if __name__ == '__main__':
    component.build_rpc_server_from_component(Worker()).serve_forever()
