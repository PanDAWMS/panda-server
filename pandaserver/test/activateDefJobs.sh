#!/bin/bash

BASEPATH=/usatlas/u/sm/prod
BINPATH=/usatlas/u/sm/latest

# for python
export PATH=$BINPATH/python/bin:$PATH
export PYTHONPATH=$BASEPATH/panda:$PYTHONPATH

python $BASEPATH/panda/test/activateDefJobs.py
