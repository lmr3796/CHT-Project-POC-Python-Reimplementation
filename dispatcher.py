#! /usr/bin/env python
import sys

import framework
import config

class Dispatcher(object):
    # TODO: implment job queuing????
    def __init__(self):
        self.workers = config.workers
        self.worker_available_status = { w: True for w in self.workers }
        return

    # TODO: Better Synchronization
    # TODO: Move it out to another class
    def lock_worker(self, worker):
        print >> sys.stderr,  'Required to lock %s,' % worker,
        if self.worker_available_status[worker]:
            self.worker_available_status[worker] = False
            print >> sys.stderr,  'success'
            return True
        print >> sys.stderr,  'fail'
        return False

    # TODO: authentication???
    def release_worker(self, worker):
        print >> sys.stderr,  'Required to release %s,' % worker,
        self.worker_available_status[worker] = True
        return


    def schedule_jobs(self, job_set):
        print >> sys.stderr,  'Dispatch request accepted'
        print >> sys.stderr,  'Worker stautus:', self.worker_available_status
        return framework.get_decision_maker().schedule_jobs(job_set, self.worker_available_status)


if __name__ == '__main__':
    framework.build_rpc_server_from_component(Dispatcher()).serve_forever()

