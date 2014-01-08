#! /usr/bin/env python

import framework
import config

class DecisionMakerByPriority(object):
    def schedule_jobs(self, job_set, worker_available_status):
        try:
            print 'Priority-based scheduling'
            sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)], key=lambda x: x[1]['priority'])
            worker_scheduled = {w: False for w, available in worker_available_status.iteritems() if available}
            schedule_result = [None] * len(job_set)
            # First, assign each job with one worker
            for i, job in sorted_job_set:
                print 'job[%d]: %s with %d tasks, priority %d' % (i,
                        job['jobname'], len(job['task']), job['priority'])
                best_worker = None
                for w in [w for w in worker_scheduled if not worker_scheduled[w]]:
                    if best_worker == None:
                        best_worker = w
                    elif job['per_server_time'][w] < job['per_server_time'][best_worker]:
                        best_worker = w
                assert best_worker != None
                schedule_result[i] = [best_worker]
                worker_scheduled[best_worker] = True
            # Second, let important jobs get as many workers as possible
            for i, job in sorted_job_set:
                worker_by_throughput = sorted([w for w in worker_scheduled if not worker_scheduled[w]],
                        key=lambda w: job['per_server_time'][w])
                if len(worker_by_throughput) == 0:
                    break
                worker_needed = min(len(worker_by_throughput), len(job['task']) - 1);
                assigned_worker = worker_by_throughput[0:worker_needed]
                for w in assigned_worker:
                    schedule_result[i].append(w)
                    worker_scheduled[w] = True
            return schedule_result

        except Exception as e:
            import traceback
            print traceback.format_exc()
            raise e

if __name__ == '__main__':
    framework.build_rpc_server_from_component(DecisionMakerByPriority(), 'DecisionMaker').serve_forever()

