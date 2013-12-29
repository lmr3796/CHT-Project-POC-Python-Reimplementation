#! /usr/bin/env python

import framework
import config

class DecisionMaker(object):

    def schedule_jobs(self, job_set, worker_available):
        try:
            print "Priority-based scheduling"
            sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)], key=lambda j: j[1]['priority'], reverse=True)
            # print "Deadline-based scheduling"
            # sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)], key=lambda j: j[1]['deadline'])
            worker_scheduled = {w: False for w, s in worker_available.iteritems() if s}
            schedule_result = [None] * len(job_set)
            # First, assign each job with one worker
            for i, job in sorted_job_set:
                print "job[" + str(i) + "]: " + job['jobname'] + " with priority " + str(job['priority'])
                worker_by_throughput = sorted([w for w in worker_scheduled if not worker_scheduled[w]],
                        key=lambda w: job['per_server_time'][w])
                worker = worker_by_throughput.pop(0)
                schedule_result[i] = [worker]
                worker_scheduled[worker] = True
            # Second, assign the rest of workers
            for i, job in sorted_job_set:
                worker_by_throughput = sorted([w for w in worker_scheduled if not worker_scheduled[w]],
                        key=lambda w: job['per_server_time'][w])
                print worker_by_throughput
                assigned_worker = self.get_required_worker_range(
                        job,
                        job['deadline'],
                        len(job['task']),
                        worker_by_throughput
                        )
                print assigned_worker
                for w in assigned_worker:
                    schedule_result[i].append(w)
                    worker_scheduled[w] = True
            return schedule_result

        except Exception as e:
            import traceback
            print traceback.format_exc()
            raise e


    def get_required_worker_range(self, job, deadline, max_worker, worker_seq):
        import operator, math
        print "Required deadline: " + str(deadline);
        print "Sequential time: " + str(job['sequential_time'])
        worker_needed = min(max_worker - 1, len(worker_seq) , int(math.ceil(job['sequential_time'] / deadline)) - 1);
        return worker_seq[0:worker_needed]

if __name__ == '__main__':
    framework.build_rpc_server_from_component(DecisionMaker()).serve_forever()

