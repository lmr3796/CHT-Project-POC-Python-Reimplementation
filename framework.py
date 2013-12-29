#! /usr/bin/env python

import socket
import xmlrpclib

import config
import job


from SimpleXMLRPCServer import SimpleXMLRPCServer
from threading import Thread


# TODO: retrieve back results
def run_task_on_worker(task, worker):
    addr = 'http://%s:%d'%(config.worker_address[worker], config.port['Worker'])
    return xmlrpclib.ServerProxy(addr).run_job(task)

def run_single_job(job_id, job, worker_set):
    def pull_job_for_worker(q, worker):
        while not q.empty():
            i, t = q.get()
            res[i] = run_task_on_worker(t, worker)
        return
    import time
    curr = time.time()
    res = [None] * len(job.get_task())
    from Queue import Queue
    task_queue = Queue()
    for i, t in enumerate(job.get_task()):
        task_queue.put((i, t))
    worker_runners = map(lambda w: Thread(target=pull_job_for_worker, args=(task_queue, w)), worker_set)
    for t in worker_runners:
        t.start()
    for t in worker_runners:
        t.join()
    ans = reduce(lambda x, y: x + int(y), res, 0);
    print 'Job[%d] "%s" finished at %6.3f seconds, %d tasks, ans=%d' % (job_id,
            job.get_name(), time.time() - curr, len(job.get_task()), ans)
    return  # Python does not support the retrieval of return value of a Thread, so any return value is meaningless

def run_job_set_by_schdule(job_set, schedule):
    assert len(job_set) == len(schedule)
    to_run = []
    for i, job, worker_set in zip(range(len(job_set)), job_set, schedule):
        t = Thread(target=run_single_job, args=(i, job, worker_set))
        to_run.append(t)
        t.start()
    for t in to_run:
        t.join()
    return


# TODO: make those components registerable?
def get_dispatcher():
    addr = 'http://%s:%d'%('localhost', config.port['Dispatcher'])
    return xmlrpclib.ServerProxy(addr, allow_none=True)
    
def get_decision_maker():
    addr = 'http://%s:%d'%('localhost', config.port['DecisionMaker'])
    return xmlrpclib.ServerProxy(addr, allow_none=True)

def build_rpc_server_from_component(comp):
    server = SimpleXMLRPCServer(('', config.port[comp.__class__.__name__]), allow_none=True)
    server.register_instance(comp)
    print "RPC Server started: %s:%d"%(comp.__class__.__name__, config.port[comp.__class__.__name__])
    return server 

# For testing RPC server
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
