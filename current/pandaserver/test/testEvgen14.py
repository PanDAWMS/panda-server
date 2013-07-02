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
    job.AtlasRelease      = 'Atlas-14.1.0'
    job.homepackage       = 'AtlasProduction/14.1.0.3'
    job.transformation    = 'csc_evgen_trf.py'
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.currentPriority   = 1000
    job.prodSourceLabel   = 'test'
    job.computingSite     = site
    job.processingType    = 'test'
    job.cmtConfig         = 'i686-slc4-gcc34-opt'
    
    file = FileSpec()
    file.lfn = "%s.evgen.pool.root" % job.jobName
    file.destinationDBlock = job.destinationDBlock
    file.destinationSE     = job.destinationSE
    file.dataset           = job.destinationDBlock
    file.destinationDBlockToken = 'ATLASDATADISK'
    file.type = 'output'
    job.addFile(file)
    
    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.destinationDBlockToken = 'ATLASDATADISK'
    fileOL.type = 'log'
    job.addFile(fileOL)
    
    job.jobParameters="5144 1 5000 1 CSC.005144.PythiaZee.py %s NONE NONE NONE" % file.lfn
    jobList.append(job)

for i in range(1):
    s,o = Client.submitJobs(jobList)
    print "---------------------"
    print s
    for x in o:
        print "PandaID=%s" % x[0]
