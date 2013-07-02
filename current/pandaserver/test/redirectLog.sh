#!/bin/bash

BASEPATH=/usatlas/u/sm/prod
BINPATH=/usatlas/u/sm/latest
LOG=$BASEPATH/httpd/logs/access_log

# for python
export PATH=$BINPATH/python/bin:$PATH
export PYTHONPATH=$BASEPATH/panda:$PYTHONPATH

tail -F $LOG | python $BASEPATH/panda/test/redirectLog.py
