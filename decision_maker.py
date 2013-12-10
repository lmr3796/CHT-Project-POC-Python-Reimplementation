#! /usr/bin/env python

import framework
import config

class DecisionMaker(object):

    def schedule_jobs(self, job_set, worker_available):
        sorted_job_set = sorted([j for j in enumerate(job_set)], lambda x: x[1].get_priority())
        worker_scheduled = {w False, for w, s in worker_available.iteritems if s}
        schedule_result = [None] * len(job_set)
        for i, job in sorted_job_set:
            workers_by_throughput = sorted([w for w in worker_scheduled if worker_scheduled[w]], key=lambda x: job.get_throughput(x), reversed=True)
            assigned_worker = self.get_required_worker_range(
                    job.get_required_throughput(),
                    job.get_max_worker(),
                    workers_by_throughput
                    )
            for w in assigned_worker:
                worker_scheduled[w] = True
            schedule_result[i] = assigned_worker
            if False not in worker_scheduled.values():  # All workers scheduled
                break


    def get_required_worker_range(self, required, max_worker, worker_seq):
        import operator.add
        for i in xrange(len(worker_seq)):
            if reduce(operator.add, worker_seq[0:i], 0.0) > required or i == max_worker:
                return worker_seq[0:i]
        return worker_seq[0:len(worker_seq)]






if __name__ == '__main__':
    framework.build_rpc_server_from_framework(DecisionMaker()).serve_forever()

