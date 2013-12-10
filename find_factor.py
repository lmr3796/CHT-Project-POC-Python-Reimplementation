#! /usr/bin/env python
import sys
import xmlrpclib

import config
import pickle
import framework 

class FindFactorJob(object):

    def __init__(self, begin, end, to_find):
        self.begin = begin
        self.end = end
        self.to_find = to_find

    def solve(self):
        total = 0
        for i in range(self.begin, self.end):
            if self.to_find % i == 0:
                total += 1
        return total

    def run(self):
        self.result = self.solve()

    def get_result(self):
        return self.result

if __name__ == '__main__':
    begin, end, to_find = map(lambda x: int(x), sys.argv[1:4])
    if len(sys.argv) <= 4 or sys.argv[4] != 'rpc':
        job = FindFactorJob(begin, end, to_find)
        job.run()
        print job.get_result()
    else:
        worker_name = config.workers[0]
        addr = 'http://%s:%d'%(config.worker_address[worker_name], config.port['Worker'])
        print xmlrpclib.ServerProxy(addr).run_job(['/bin/ls', '-al'])
