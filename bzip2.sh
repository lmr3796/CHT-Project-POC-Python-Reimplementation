#! /usr/bin/env bash


SPEC_ROOT=${SPEC_ROOT:=/home/lmr3796/SPEC_BUILD}
BZIP2_PATH=$SPEC_ROOT/log/benchspec/CPU2006/401.bzip2/run/run_base_train_amd64-m64-gcc43-nn.0000

cd $SPEC_ROOT
source shrc
cd $BZIP2_PATH
./run.sh

