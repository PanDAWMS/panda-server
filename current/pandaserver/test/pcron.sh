#!/bin/bash

"exec" "python" "$0" "$@"

import os
import sys
import time
import commands

_python = "/direct/usatlas+u/gfg/python-latest/python-2.4.1/python-2.4.1/bin/python"

class Woker:
    # constructor
    def __init__(self):
        pass

    # main
    def run(self):
        os.chdir('/direct/usatlas+u/sm/panda/pilot2')
	com = "python pilot.py -a /usatlas/projects/OSG -d /tmp -l /usatlas/prodjob/share/ -q http://dms02.usatlas.bnl.gov:8000/dq2/ -s BNL_ATLAS_DDM"
        os.spawnv(os.P_NOWAIT,_python,com.split())

# count # of processes
out = commands.getoutput('ps auxww | grep pilot.py | grep -v auxww | grep -v "sh -c" | grep -v grep' )
if out == '':
    nPilot = 0
else:
    nPilot = len(out.split('\n'))
maxPilot = 10
print nPilot
if nPilot >= maxPilot:
    sys.exit(0)

for i in range(maxPilot-nPilot):
    thr = Woker()
    thr.run()
    time.sleep(5)
