#
# eg. python cl_testEvgen.py SACLAY FR
# 
import sys
import time
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

if len(sys.argv)==2:
    site = sys.argv[1]
    cloud='CA'
elif  len(sys.argv)==3:
    site = sys.argv[1]
    cloud=sys.argv[2]    
else:
    site = None
    cloud = None
    
datasetName = 'testpanda.destDB.%s_tid999991' % commands.getoutput('uuidgen')
taskid = 999989

jobList = []

for i in range(1):
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = "%s_%d" % (commands.getoutput('uuidgen'),i)
#    job.AtlasRelease      = 'Atlas-12.0.6'
#    job.homepackage       = 'AtlasProduction/12.0.6.5'
    job.AtlasRelease      = 'Atlas-12.0.7'
    job.homepackage       = 'AtlasProduction/12.0.7.1'

    job.transformation    = 'csc_evgen_trf.py'
    job.destinationDBlock = datasetName
#    job.destinationSE     = destName
#    job.cloud             = 'CA'
    job.cloud             = cloud
    job.taskID = taskid
    job.currentPriority   = 1000
    job.prodSourceLabel   = 'test'
#    job.prodSourceLabel   = 'cloudtest'
    job.computingSite     = site
    
    file = FileSpec()
    file.lfn = "%s.evgen.pool.root" % job.jobName
    file.destinationDBlock = job.destinationDBlock
    file.destinationSE     = job.destinationSE
    file.dataset           = job.destinationDBlock
    file.type = 'output'
    job.addFile(file)
    
    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)
    
    job.jobParameters="8072 0 5000 1 DC3.008072.JimmyPhotonJet1.py %s NONE NONE NONE" % file.lfn
    jobList.append(job)

for i in range(1):
    s,o = Client.submitJobs(jobList)
    print "---------------------"
    print s
    for x in o:
        print "PandaID=%s" % x[0]
