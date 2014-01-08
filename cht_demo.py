#! /usr/bin/env python
import time
import sys
from threading import Thread

import framework
import config
from job import Job


RUN_PATH = '/home/lmr3796/CHT-Project-POC-Python-Reimplementation'

BZIP2_PER_TASK_RUNNING_TIME = 42
H264_PER_TASK_RUNNING_TIME = 80
WC_PER_TASK_RUNNING_TIME = 14

def main():
    total_deadline = int(sys.argv[1])
    delay = int(sys.argv[2])

    wc_job = Job('word count')
    bzip2_job = Job('bzip2')
    h264_job = Job('h264')

    for i in range(20):
        wc_job.add_task(RUN_PATH + '/word_count.sh')
    for i in range(10):
        bzip2_job.add_task(RUN_PATH + '/bzip2.sh')
    for i in range(4):
        h264_job.add_task(RUN_PATH + '/h264.sh')

    for worker in config.workers:
        wc_job.set_per_server_time(worker, WC_PER_TASK_RUNNING_TIME)
        bzip2_job.set_per_server_time(worker, BZIP2_PER_TASK_RUNNING_TIME)
        h264_job.set_per_server_time(worker, H264_PER_TASK_RUNNING_TIME)

    wc_job.set_priority(1)
    bzip2_job.set_priority(2)
    h264_job.set_priority(3)

    # First batch
    print 'Dispatching first batch'
    job_set = [wc_job]
    for j in job_set:
        j.set_deadline(total_deadline)
    schedule = framework.get_dispatcher().schedule_jobs(job_set)
    print schedule
    t1 = Thread(target=framework.run_job_set, args=(job_set,))
    t1.start()

    time.sleep(delay)


    # Second batch
    print 'Dispatching second batch'
    job_set = [bzip2_job, h264_job]
    for j in job_set:
        j.set_deadline(total_deadline - delay)
    schedule = framework.get_dispatcher().schedule_jobs(job_set)
    print schedule
    t2 = Thread(target=framework.run_job_set, args=(job_set,))
    t2.start()

    # Wait till finish
    t1.join()
    t2.join()

    return 0

if __name__ == '__main__':
    exit(main())
