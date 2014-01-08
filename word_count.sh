#! /usr/bin/env bash

for (( i=0 ; $i<40 ; i=i+1 ))
do
    wc -w ~/CHT/SPEC_2006/build/benchspec/CPU2006/401.bzip2/data/all/input/input.combined > /dev/null
done
