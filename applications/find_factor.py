#! /usr/bin/env python
import sys

class FindFactorJob(object):

    def __init__(self, begin, end, to_find):
        self.begin = begin
        self.end = end
        self.to_find = to_find

    def solve(self):
        total = 0
        for i in range(self.begin, self.end):
            if self.to_find % i == 0:
                total += 1
        return total

    def run(self):
        self.result = self.solve()

    def get_result(self):
        return self.result

def main():
    begin, end, to_find = map(lambda x: int(x), sys.argv[1:])
    job = FindFactorJob(begin, end, to_find)
    job.run()
    print job.get_result()
    return 0

if __name__ == '__main__':
    exit(main())
