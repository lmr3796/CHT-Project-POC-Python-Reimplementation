#! /usr/bin/env python

class Job(object):
    def __init__(self, name):
        self.jobname = name
        self.task = []
        self.priority = 0
        self.required_throughput = 0.0
        self.single_server_throughput = 0.0
        self.per_server_throughput = {}

    # Serve as a exec command (command, ARGV1, ARGV2.....)
    def add_task(self, cmd, *arg):
        self.task.append([cmd] + list(arg))
        return

    def get_name(self):
        return self.jobname

    def get_task(self):
        return self.task

    def clear_task(self):
        self.task = []

    def get_max_worker(self):
        return len(self, task)

    def set_single_server_throughput(self, throughput):
        self.single_server_throughput = throughput

    def set_per_server_throughput(self, worker, throughput):
        self.per_server_throughput[worker] = throughput

    def set_required_throughput(self, required_throughput):
        self.required_throughput = required_throughput
        return 

    def get_required_throughput(self):
        return required_throughput

    # higher value means higher priority
    def set_priority(self, priority):
        self.priority = priority
        return

    def get_priority(self, priority):
        return self.priority


if __name__ == '__main__':
    pass
