import sys

import userinterface.Client as Client

if len(sys.argv) == 2:
    Client.resubmitJobs([sys.argv[1]])
else:
    startID = int(sys.argv[1])
    endID   = int(sys.argv[2])
    if startID > endID:
        print '%d is less than %d' % (endID,startID)
        sys.exit(1)
    Client.resubmitJobs(range(startID,endID+1))

