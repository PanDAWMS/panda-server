import sys
import time
import commands
import userinterface.Client as Client

id = sys.argv[1]

s,o = Client.getJobStatus([id])
print s
if s == 0:
    for job in o:
        if job == None:
            continue
        print job.PandaID
        for file in job.Files:
            print file.lfn,file.type

