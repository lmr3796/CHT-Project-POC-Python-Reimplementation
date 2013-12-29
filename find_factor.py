#! /usr/bin/env python
import sys
import socket
import xmlrpclib

import config
import framework 
from job import Job

THREAD_TO_USE = 8
SINGLE_THREAD_TIME = 18.0  # find_factor.py 1 200000001 200000000

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
    if len(sys.argv) != 4 and len(sys.argv) != 6:
        print "Usage: find_factor.py BEGIN END TARGET_NUMBER"
        print "   or  find_factor.py BEGIN END TARGET_NUMBER TIME_LIMIT RUNNING_MODE"
        exit()
    begin, end, to_find = map(lambda x: int(x), sys.argv[1:4])
    solver = FindFactorSolver(begin, end, to_find)
    if len(sys.argv) == 4:
        solver.run()
        print solver.get_result()
    else:
        assert sys.argv[-1] in ('dry-run', 'rpc')
        deadline = float(sys.argv[4])
        job_set = [Job('Loose job'), Job('Tight job')]

        # Loose job
        for r in solver.split_ranges(2):
            job_set[0].add_task(__file__, str(r[0]), str(r[1]), str(to_find))
        for idx, worker in enumerate(config.workers):
            job_set[0].set_per_server_time(worker, SINGLE_THREAD_TIME - idx)
        print job_set[0].per_server_time
        job_set[0].set_priority(4)
        job_set[0].set_sequential_time(SINGLE_THREAD_TIME * 3)
        job_set[0].set_deadline(deadline)

        # Tight job
        for r in solver.split_ranges(THREAD_TO_USE):
            job_set[1].add_task(__file__, str(r[0]), str(r[1]), str(to_find))
        for idx, worker in enumerate(config.workers):
            job_set[1].set_per_server_time(worker, SINGLE_THREAD_TIME + idx)
        print job_set[1].per_server_time
        job_set[1].set_priority(2)
        job_set[1].set_sequential_time(SINGLE_THREAD_TIME)
        job_set[1].set_deadline(deadline/2)  # The tighter job simply halfs the deadline

        framework.get_decision_maker().set_scheduling_policy('Workload')
        schedule = framework.get_dispatcher().dispatch_job(job_set)
        print schedule
        if sys.argv[-1] != 'dry-run':
            framework.run_job_set_by_schdule(job_set, schedule)
