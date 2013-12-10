#! /usr/bin/env python
import sys
import socket
import xmlrpclib

import config
import framework 
from job import Job

THREAD_TO_USE = 8
SINGLE_THREAD_TIME = 2.46

class FindFactorSolver(object):

    def __init__(self, begin, end, to_find):
        self.begin = begin
        self.end = end
        self.to_find = to_find

    def solve(self):
        total = 0
        for i in xrange(self.begin, self.end):
            if self.to_find % i == 0:
                total += 1
        return total

    def run(self):
        self.result = self.solve()

    def get_result(self):
        return self.result

    def split_ranges(self, threads):
        res = []
        num_per_thread = float(self.end - self.begin) / threads
        curr = self.begin
        for i in range(threads):
            res.append((int(curr), int(curr+num_per_thread)))
            curr += num_per_thread
        return res

if __name__ == '__main__':
    begin, end, to_find = map(lambda x: int(x), sys.argv[1:4])
    solver = FindFactorSolver(begin, end, to_find)
    if len(sys.argv) <= 5:
        solver.run()
        print solver.get_result()
    else:
        assert sys.argv[-1] == 'rpc'
        deadline = float(sys.argv[4])
        job_set = [Job('Loose job'), Job('Tight job')]
        for r in solver.split_ranges(THREAD_TO_USE):
            job_set[0].add_task(__file__, str(r[0]), str(r[1]), str(to_find))
        job_set[0].set_single_server_throughput(1.0/SINGLE_THREAD_TIME)
        job_set[0].set_required_throughput(THREAD_TO_USE/deadline)
        for r in solver.split_ranges(THREAD_TO_USE):
            job_set[1].add_task(__file__, str(r[0]), str(r[1]), str(to_find))
        job_set[1].set_single_server_throughput(1.0/SINGLE_THREAD_TIME)
        job_set[1].set_required_throughput(THREAD_TO_USE/deadline*2)

        schedule = framework.get_dispatcher().dispatch_job(job_set)
        print schedule
        framework.run_job_set_by_schdule(job_set, schedule)


        #print xmlrpclib.ServerProxy(addr).dispatch_job()

        '''
        worker_name = config.workers[0]
        addr = 'http://%s:%d'%(config.worker_address[worker_name], config.port['Worker'])
        print xmlrpclib.ServerProxy(addr).run_job([__file__, str(begin), str(end), str(to_find)]).strip()
        '''
