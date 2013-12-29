#! /usr/bin/env python

class Job(object):
    def __init__(self, name):
        self.jobname = name
        self.task = []
        self.priority = 0
        self.deadline = 0.0
        self.sequential_time = 0.0
        self.per_server_time = {}

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

    def set_sequential_time(self, sequential_time):
        self.sequential_time = sequential_time

    def set_per_server_time(self, worker, time):
        self.per_server_time[worker] = time

    def set_deadline(self, deadline):
        self.deadline = deadline
        return 

    def get_deadline(self):
        return deadline

    # higher value means higher priority
    def set_priority(self, priority):
        self.priority = priority
        return

    def get_priority(self, priority):
        return self.priority


if __name__ == '__main__':
    pass
