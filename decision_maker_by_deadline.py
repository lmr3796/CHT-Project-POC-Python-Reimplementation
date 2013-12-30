#! /usr/bin/env python

import framework
import config

class DecisionMakerByDeadline(object):
    def schedule_jobs(self, job_set, worker_available):
        try:
            print 'Deadline-based scheduling'
            # Concept: make jobs with higher priority meet their deadlines
            sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)],
                    key=lambda x: x[1]['priority'], reverse=True)
            worker_scheduled = {w: False for w, available in worker_available.iteritems() if available}
            schedule_result = [None] * len(job_set)
            # First, assign each job with one worker
            for i, job in sorted_job_set:
                print 'job[%d]: %s with %d tasks, priority %d, seq_time %.2f, deadline %.2f' % (i,
                        job['jobname'], len(job['task']), job['priority'], job['sequential_time'], job['deadline'])
                best_worker = None
                for w in [w for w in worker_scheduled if not worker_scheduled[w]]:
                    if best_worker == None:
                        best_worker = w
                    elif job['per_server_time'][w] < job['per_server_time'][best_worker]:
                        best_worker = w
                assert best_worker != None
                schedule_result[i] = [best_worker]
                worker_scheduled[best_worker] = True
            # Second, let important jobs meet their deadline in optimal workers
            for i, job in sorted_job_set:
                worker_by_throughput = sorted([w for w in worker_scheduled if not worker_scheduled[w]],
                        key=lambda w: job['per_server_time'][w])
                assigned_worker = self.get_required_worker_range(
                        job,
                        job['deadline'],
                        len(job['task']),
                        worker_by_throughput)
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
        worker_needed = min(len(worker_seq), max_worker - 1, int(math.ceil(job['sequential_time'] / deadline)) - 1);
        return worker_seq[0:worker_needed]

if __name__ == '__main__':
    framework.build_rpc_server_from_component(DecisionMakerByDeadline(), 'DecisionMaker').serve_forever()

