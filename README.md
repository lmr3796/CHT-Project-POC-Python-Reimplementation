CHT-Project-POC-Python-Reimplementation
=======================================

1.  Data must be shared by NFS
2.  Edit config.py for worker information
3.  Run ./worker.py on each worker node
4.  Run ./dispatcher.py on coordinator
5.  Run ./find_factor.py on coordinator
    - Usage: ./find_factor.py BEGIN END TARGET_NUMBER RUNNING_MODE<br/>
      ex:

            ./find_factor.py 1 200000001 200000000 rpc
      Note that the range to find is [BEGIN, END), so END must be the greatest number you like +1
    - RUNNING_MODE can be `rpc` or `dry-run`
        1. rpc: dispatch jobs
        2. dry-run: generates schdule output only
