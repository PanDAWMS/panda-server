import sys

import userinterface.Client as Client

aSrvID = None

codeV = None
argv = []
for arg in sys.argv:
    if arg == '-9':
        codeV = 9
    else:
        argv.append(arg)

sys.argv =argv

if len(sys.argv) == 2:
    Client.killJobs([sys.argv[1]],code=codeV)
else:
    startID = int(sys.argv[1])
    endID   = int(sys.argv[2])
    if startID > endID:
        print '%d is less than %d' % (endID,startID)
        sys.exit(1)
    Client.killJobs(range(startID,endID+1),code=codeV)

