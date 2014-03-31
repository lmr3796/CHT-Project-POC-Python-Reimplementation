#! /usr/bin/env bash

SPEC_ROOT=${SPEC_ROOT:=/home/lmr3796/SPEC_BUILD}
for (( i=0 ; $i<40 ; i=i+1 ))
do
    wc -w $SPEC_ROOT/benchspec/CPU2006/401.bzip2/data/all/input/input.combined #> /dev/null
done
