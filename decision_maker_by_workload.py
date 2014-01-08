#! /usr/bin/env python
import sys

import framework
import config

class DecisionMakerByWorkload(object):
    def schedule_jobs(self, job_set, worker_available_status):
        try:
            print >> sys.stderr, 'Workload-based scheduling'
            remaining_worker = [w for w, available in worker_available_status.iteritems() if available]
            workload = [len(j['task']) * sum(j['per_server_time'].values()) / float(len(j['per_server_time'])) for j in job_set]
            workload = map(lambda load: len(remaining_worker) * load / sum(workload), workload)  # Normalized to total of #remaining_worker
            print >> sys.stderr, 'Workload:', workload
            sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)], key=lambda x: workload[x[0]])
            schedule_result = [[] for i in  range(len(job_set))]

            # First, assign each job with one worker
            for i, job in sorted_job_set:
                print >> sys.stderr, 'Job[%d]: %s with %d tasks, workload %.2f' % (i, job['jobname'], len(job['task']), workload[i])
                best_worker = min(remaining_worker, key=lambda w:job['per_server_time'][w])
                schedule_result[i].append(best_worker)
                remaining_worker.remove(best_worker)
                workload[i] = workload[i] - 1.0 # TODO: WHAT????????


            # Second, assign the rest of workers by their workload
            while remaining_worker:
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
                best_worker = min(remaining_worker, key=lambda w:job['per_server_time'][w])
                schedule_result[heaviest_job_id].append(best_worker)
                remaining_worker.remove(best_worker)
                workload[heaviest_job_id] = workload[heaviest_job_id] - 1.0 # TODO: WHAT????????
            return schedule_result

        except Exception as e:
            import traceback
            print traceback.format_exc()
            raise e

if __name__ == '__main__':
    framework.build_rpc_server_from_component(DecisionMakerByWorkload(), 'DecisionMaker').serve_forever()

