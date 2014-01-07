CHT-Project-POC-Python-Reimplementation
=======================================

## Deploy cluster
1.  Data must be shared by NFS
2.  Edit `./config.py` for worker information
3.  Run `./worker.py` on each worker node
4.  Run `./dispatcher.py` on coordinator
5.  Run `./decision_maker_by_deadline.py` on coordinator; you can also use other decision makers instead.

## Run application
*  Run `./find_factor.py` on coordinator

`./find_factor.py BEGIN END TARGET_NUMBER DEADLINE RUNNING_MODE`  
e.g. `./find_factor.py 1 200000001 200000000 32 rpc`

**Note** that the range to find is **[BEGIN, END)**, so END must be the greatest number
you like +1

* `TARGET_NUMBER`: target number to find factors
* `DEADLINE`: time constraint in seconds
* `RUNNING_MODE` can be `rpc` or `dry-run`
    1. `rpc`: dispatch jobs
    2. `dry-run`: generates schdule output only
