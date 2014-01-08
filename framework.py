#! /usr/bin/env python

import sys
import socket
import time
import xmlrpclib
from Queue import Queue

import config
import job
from SimpleXMLRPCServer import SimpleXMLRPCServer
from threading import Thread


MAX_LOCK_RETRY = 3

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
    curr = time.time()
    res = [None] * len(job.get_task())
    task_queue = Queue()
    for i, t in enumerate(job.get_task()):
        task_queue.put((i, t))
    worker_runners = map(lambda w: Thread(target=pull_job_for_worker, args=(task_queue, w)), worker_set)
    for t in worker_runners:
        t.start()
    for t in worker_runners:
        t.join()

    # Release workers after job done
    for w in worker_set:
        get_dispatcher().release_worker(w)

    print 'Job[%d] "%s" finished in %6.3f seconds, %d tasks' % (job_id,
            job.get_name(), time.time() - curr, len(job.get_task()))
    return  # Python does not support the retrieval of return value of a Thread, so any return value is meaningless

def run_job_set_by_schdule(job_set, schedule, print_schedule):
    assert len(job_set) == len(schedule)

    def release_all(schedule):
        dispatcher = get_dispatcher()
        for s in schedule:
            for w in s:
                dispatcher.release_worker(w)
        return

    def lock_all(schedule):
        dispatcher = get_dispatcher()
        for s in schedule:
            for w in s:
                if not dispatcher.lock_worker(w):
                    return False
        return True


    # Try to lock required workers
    if not lock_all(schedule):
        release_all(schedule)
        return False


    if print_schedule:
        print >> sys.stderr, schedule

    # Run each job in a thread
    to_run = []
    for i, job, worker_set in zip(range(len(job_set)), job_set, schedule):
        t = Thread(target=run_single_job, args=(i, job, worker_set))
        to_run.append(t)
        t.start()

    # Wait till all job done
    for t in to_run:
        t.join()
    return True

def run_job_set(job_set, print_schedule=False):
    dispatcher = get_dispatcher()
    for i in range(MAX_LOCK_RETRY):
        schedule = dispatcher.schedule_jobs(job_set)
        if run_job_set_by_schdule(job_set, schedule, print_schedule):
            return

    print >> sys.stderr, 'Fail to lock required workers'
    return


# TODO: make those components registerable?
def get_dispatcher():
    addr = 'http://%s:%d'%('localhost', config.port['Dispatcher'])
    return xmlrpclib.ServerProxy(addr, allow_none=True)
    
def get_decision_maker():
    addr = 'http://%s:%d'%('localhost', config.port['DecisionMaker'])
    return xmlrpclib.ServerProxy(addr, allow_none=True)

def build_rpc_server_from_component(comp, service_name=None):
    if service_name == None:
        service_name = comp.__class__.__name__
    server = SimpleXMLRPCServer(('', config.port[service_name]), allow_none=True)
    server.register_instance(comp)
    print "RPC Server started: %s:%d"%(comp.__class__.__name__, config.port[service_name])
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
