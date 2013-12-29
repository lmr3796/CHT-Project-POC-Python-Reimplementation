#! /usr/bin/env python

import framework
import config

class DecisionMaker(object):

    def schedule_jobs(self, job_set, worker_available):
        try:
            sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)], key=lambda x: x[1]['priority'], reverse=True)
            worker_scheduled = {w: False for w, s in worker_available.iteritems() if s}
            schedule_result = [None] * len(job_set)
            for i, job in sorted_job_set:
                worker_by_throughput = sorted([w for w in worker_scheduled if not worker_scheduled[w]],
                        key=lambda x: job['single_server_throughput'], reverse=True)
                assigned_worker = self.get_required_worker_range(
                        job,
                        job['required_throughput'],
                        len(job['task']),
                        worker_by_throughput
                        )
                for w in assigned_worker:
                    worker_scheduled[w] = True
                schedule_result[i] = assigned_worker
            return schedule_result

        except Exception as e:
            import traceback
            print traceback.format_exc()
            raise e


    def get_required_worker_range(self, job, required, max_worker, worker_seq):
        import operator, math
        worker_needed = min(max_worker, len(worker_seq) , int(math.ceil(required / job['single_server_throughput'])))
        return worker_seq[0:worker_needed]

if __name__ == '__main__':
    framework.build_rpc_server_from_component(DecisionMaker()).serve_forever()

