#! /usr/bin/env bash


SPEC_ROOT=${SPEC_ROOT:=/home/lmr3796/SPEC_BUILD}
H264_PATH=$SPEC_ROOT/log/benchspec/CPU2006/464.h264ref/run/run_base_train_amd64-m64-gcc43-nn.0000

cd $SPEC_ROOT
source shrc
cd $H264_PATH
./run.sh

