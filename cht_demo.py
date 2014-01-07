#! /usr/bin/env python


RUN_PATH = /home/lmr3796/CHT-Project-POC-Python-Reimplementation

BZIP2_RUNNING_TIME = 42
H264_RUNNING_TIME = 79

def main():
    bzip2_job = Job('bzip2')
    h264_job = Job('h264')

    for i in range(10):
        bzip2_job.add_task(RUN_PATH + '/bzip2.sh')
    for i in range(4):
        h264_job.add_task(RUN_PATH + '/h264.sh')

    bzip2_job.set_single_server_throughput(1.0/BZIP2_RUNNING_TIME)
    h264_job.set_single_server_throughput(1.0/H264_RUNNING_TIME)






    return 0

if __name__ == '__main__':
    exit(main())
