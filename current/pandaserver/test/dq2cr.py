import sys
import time
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

if len(sys.argv)>1:
    site = sys.argv[1]
else:
    site = None

datasetName = 'panda.destDB.%s' % commands.getoutput('uuidgen')
destName    = 'BNL_SE'

jobList = []

for i in range(1):
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = "%s_%d" % (commands.getoutput('uuidgen'),i)
    job.transformation    = 'https://gridui01.usatlas.bnl.gov:24443/dav/test/run_dq2_cr'
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.currentPriority   = 100000
    #job.prodSourceLabel   = 'test'
    job.prodSourceLabel   = 'user'
    job.computingSite     = site
    
    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)
    
    job.jobParameters="8072 0 5000 1 DC3.008072.JimmyPhotonJet1.py NONE NONE NONE"
    jobList.append(job)

s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
