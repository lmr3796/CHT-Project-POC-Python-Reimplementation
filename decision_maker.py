#! /usr/bin/env python

import framework
import config

class DecisionMaker(object):
    def __init__(self):
        self.scheduling_policy = config.scheduling_policy

    def set_scheduling_policy(self, policy):
        self.scheduling_policy = policy

    def schedule_jobs(self, job_set, worker_available):
        if self.scheduling_policy == 'Priority':
            return self.schedule_by_priority(job_set, worker_available)
        elif self.scheduling_policy == 'Workload':
            return self.schedule_by_workload(job_set, worker_available)
        else: # Default is scheduling by deadline
            return self.schedule_by_deadline(job_set, worker_available)

    def schedule_by_priority(self, job_set, worker_available):
        try:
            print 'Priority-based scheduling'
            sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)],
                    key=lambda x: x[1]['priority'], reverse=True)
            worker_scheduled = {w: False for w, available in worker_available.iteritems() if available}
            schedule_result = [None] * len(job_set)
            # First, assign each job with one worker
            for i, job in sorted_job_set:
                best_worker = None
                for w in [w for w in worker_scheduled if not worker_scheduled[w]]:
                    if best_worker == None:
                        best_worker = w
                    elif job['per_server_time'][w] < job['per_server_time'][best_worker]:
                        best_worker = w
                assert best_worker != None
                schedule_result[i] = [best_worker]
                worker_scheduled[best_worker] = True
            # Second, assign the rest of workers
            for i, job in sorted_job_set:
                print 'job[%d]: %s with %d tasks, priority %d' % (i,
                        job['jobname'], len(job['task']), job['priority'])
                worker_by_throughput = sorted([w for w in worker_scheduled if not worker_scheduled[w]],
                        key=lambda w: job['per_server_time'][w])
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

    def schedule_by_workload(self, job_set, worker_available):
        try:
            print 'Workload-based scheduling'
            total_workload = reduce(lambda x, y: x + y['sequential_time'], job_set, 0.0)
            workload = [j['sequential_time'] for j in job_set]
            for idx in range(len(workload)):
                workload[idx] = len(worker_available) * workload[idx] / total_workload
            sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)],
                    key=lambda x: workload[x[0]], reverse=True)
            worker_scheduled = {w: False for w, available in worker_available.iteritems() if available}
            schedule_result = [None] * len(job_set)
            # First, assign each job with one worker
            for i, job in sorted_job_set:
                print 'job[%d]: %s with %d tasks, workload %f' % (i,
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
            # Second, assign the rest of workers
            while len([w for w in worker_scheduled if not worker_scheduled[w]]) > 0:
                heaviest_job_id = None
                for idx in range(len(workload)):
                    if len(job_set[idx]['task']) == len(schedule_result[idx]):
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

    def schedule_by_deadline(self, job_set, worker_available):
        def get_required_worker_range(job, deadline, max_worker, worker_seq):
            import operator, math
            print 'Sequential time: ' + str(job['sequential_time'])
            worker_needed = min(len(worker_seq), max_worker - 1, int(math.ceil(job['sequential_time'] / deadline)) - 1);
            return worker_seq[0:worker_needed]

        try:
            print 'Deadline-based scheduling'
            sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)],
                    key=lambda x: x[1]['deadline'])
            worker_scheduled = {w: False for w, available in worker_available.iteritems() if available}
            schedule_result = [None] * len(job_set)
            # First, assign each job with one worker
            for i, job in sorted_job_set:
                worker_by_throughput = sorted([w for w in worker_scheduled if not worker_scheduled[w]],
                        key=lambda w: job['per_server_time'][w])
                worker = worker_by_throughput.pop(0)
                schedule_result[i] = [worker]
                worker_scheduled[worker] = True
            # Second, assign the rest of workers
            for i, job in sorted_job_set:
                print 'job[%d]: %s with %d tasks, deadline %f' % (i,
                        job['jobname'], len(job['task']), job['deadline'])
                worker_by_throughput = sorted([w for w in worker_scheduled if not worker_scheduled[w]],
                        key=lambda w: job['per_server_time'][w])
                assigned_worker = get_required_worker_range(
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

if __name__ == '__main__':
    framework.build_rpc_server_from_component(DecisionMaker()).serve_forever()

