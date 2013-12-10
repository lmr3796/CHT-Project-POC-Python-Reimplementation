#! /usr/bin/env python
import sys

def find_factor_cnt(begin, end, to_find):
    total = 0
    for i in range(begin, end):
        if to_find % i == 0:
            total += 1
    return total
        
def main():
    begin = int(sys.argv[1])
    end = int(sys.argv[2])
    to_find = int(sys.argv[3])
    print begin, end, to_find
    print find_factor_cnt(begin, end, to_find)
    return 0

if __name__ == '__main__':
    exit(main())
