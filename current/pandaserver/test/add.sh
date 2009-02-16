#!/bin/bash

# Panda home
export PANDA_HOME=/home/sm/prod

# for python
export PYTHONPATH=$PANDA_HOME/panda:$PYTHONPATH

python $PANDA_HOME/panda/test/add.py
