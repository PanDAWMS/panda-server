import sys

import userinterface.Client as Client

aSrvID = None

for idx,argv in enumerate(sys.argv):
    if argv == '-s':
        aSrvID = sys.argv[idx+1]
        sys.argv = sys.argv[:idx]
        break

if len(sys.argv) == 2:
    Client.killJobs([sys.argv[1]],srvID=aSrvID)
else:
    startID = int(sys.argv[1])
    endID   = int(sys.argv[2])
    if startID > endID:
        print '%d is less than %d' % (endID,startID)
        sys.exit(1)
    Client.killJobs(range(startID,endID+1),srvID=aSrvID)

