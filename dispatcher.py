#! /usr/bin/env python

import framework
import config

from decision_maker import DecisionMaker

class Dispatcher(object):
    # TODO: implment job queuing????
    def __init__(self):
        self.workers = config.workers
        self.decision_maker = DecisionMaker()
        self.worker_available = { w: True for w in self.workers }
        return

    # TODO: Better Synchronization
    # TODO: Move it out to another class
    def lock_worker(self, worker):
        if self.worker_available[worker]:
            self.worker_available[worker] = False
            return True
        return False

    # TODO: authentication???
    def release_worker(self, worker):
        self.worker_available[worker] = True
        return


    def dispatch_job(self, job_set):
        return self.decision_maker.schedule_jobs(job_set, self.worker_available)


if __name__ == '__main__':
    framework.build_rpc_server_from_component(Dispatcher()).serve_forever()

