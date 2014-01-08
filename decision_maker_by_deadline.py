#! /usr/bin/env python

import framework
import config

class DecisionMakerByDeadline(object):
    def schedule_jobs(self, job_set, worker_available_status):
        try:
            print 'Deadline-based scheduling'
            print 'Available worker: %s' % worker_available_status
            # Concept: make jobs with higher priority meet their deadlines
            sorted_job_set = sorted([(idx, j) for idx, j in enumerate(job_set)],
                    key=lambda x: x[1]['priority'], reverse=True)
            remaining_worker = [w for w, available in worker_available_status.iteritems() if available]
            schedule_result = [[] for i in range(len(job_set))] # Don't use [[]] * len(), contents would be the same reference
            # Let important jobs meet their deadline in optimal workers
            for i, job in sorted_job_set:
                worker_by_throughput = sorted(remaining_worker, key=lambda w: job['per_server_time'][w])
                assigned_worker_range = self.get_required_worker_range(job, job['deadline'], worker_by_throughput)
                assigned_worker_range[1] = min(assigned_worker_range[1], len(job['task']))  # No use if #worker > #tasks
                schedule_result[i] += remaining_worker[slice(*assigned_worker_range)]
                del remaining_worker[slice(*assigned_worker_range)]
            return schedule_result

        except Exception as e:
            import traceback
            print traceback.format_exc()
            raise e

    def get_required_worker_range(self, job, deadline, worker_by_throughput):
        needed_worker = 0
        total_throughput = 0.0
        required_throughput = len(job['task']) * 1.0 / deadline
        print 'Deadline: %d' % deadline
        print 'Required: %f' % required_throughput
        for w in worker_by_throughput:
            if total_throughput >= required_throughput:
                break
            needed_worker += 1
            total_throughput += 1.0 / job['per_server_time'][w]
            print 'Added %s, total throughput increased %f, now %f' % (w, 1.0 / job['per_server_time'][w], total_throughput)
        return [0, needed_worker]

if __name__ == '__main__':
    framework.build_rpc_server_from_component(DecisionMakerByDeadline(), 'DecisionMaker').serve_forever()

