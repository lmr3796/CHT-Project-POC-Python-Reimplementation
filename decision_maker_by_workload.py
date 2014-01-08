#! /usr/bin/env python
import sys

import framework
import config

class DecisionMakerByWorkload(object):
    def schedule_jobs(self, job_set, worker_available_status):
        try:
            print >> sys.stderr, 'Workload-based scheduling'
            workload = [sum(j['per_server_time'].values())/float(len(j['per_server_time'])) for j in job_set]
            total_workload = sum(workload)
            print >> sys.stderr, 'Workload:',  workload
            print >> sys.stderr, 'Total workload:',  total_workload
            for idx in range(len(workload)):
                workload[idx] = len(worker_available_status) * workload[idx] / total_workload
            sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)],
                    key=lambda x: workload[x[0]], reverse=True)
            worker_scheduled = {w: False for w, available in worker_available_status.iteritems() if available}
            schedule_result = [None] * len(job_set)
            # First, assign each job with one worker
            for i, job in sorted_job_set:
                print 'job[%d]: %s with %d tasks, workload %.2f' % (i,
                        job['jobname'], len(job['task']), workload[i])
                best_worker = None
                for w in [w for w in worker_scheduled if not worker_scheduled[w]]:
                    if best_worker == None:
                        best_worker = w
                    elif job['per_server_time'][w] < job['per_server_time'][best_worker]:
                        best_worker = w
                assert best_worker != None
                schedule_result[i] = [best_worker]
                workload[i] = workload[i] - 1.0
                worker_scheduled[best_worker] = True
            # Second, assign the rest of workers by their workload
            while len([w for w in worker_scheduled if not worker_scheduled[w]]) > 0:
                heaviest_job_id = None
                for idx in range(len(workload)):
                    if len(job_set[idx]['task']) == len(schedule_result[idx]):
                        # The number of worker is equal to the number of tasks, no more workers needed
                        continue
                    if heaviest_job_id == None:
                        heaviest_job_id = idx
                    elif workload[idx] > workload[heaviest_job_id]:
                        heaviest_job_id = idx
                if heaviest_job_id == None:
                    break
                job = job_set[heaviest_job_id]
                best_worker = None
                for w in [w for w in worker_scheduled if not worker_scheduled[w]]:
                    if best_worker == None:
                        best_worker = w
                    elif job['per_server_time'][w] < job['per_server_time'][best_worker]:
                        best_worker = w
                assert best_worker != None
                schedule_result[heaviest_job_id].append(best_worker)
                workload[heaviest_job_id] = workload[heaviest_job_id] - 1.0
                worker_scheduled[best_worker] = True
            return schedule_result

        except Exception as e:
            import traceback
            print traceback.format_exc()
            raise e

if __name__ == '__main__':
    framework.build_rpc_server_from_component(DecisionMakerByWorkload(), 'DecisionMaker').serve_forever()

