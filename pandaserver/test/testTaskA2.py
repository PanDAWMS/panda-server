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

datasetName = 'testpanda.destDB.%s' % commands.getoutput('uuidgen')
#destName   = 'BNL_SE'

jobList = []

for i in [999905,999906,999907]:
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = "%s_%d" % (commands.getoutput('uuidgen'),i)
    job.AtlasRelease      = 'Atlas-14.1.0'
    job.homepackage       = 'AtlasProduction/12.0.6.2'
    job.transformation    = 'csc_evgen_trf.py'
    job.destinationDBlock = datasetName
    #job.destinationSE     = destName
    job.currentPriority   = 1000
    job.prodSourceLabel   = 'managed'
    #job.prodSourceLabel   = 'test'
    #job.computingSite     = site
    job.cmtConfig         = 'i686-slc4-gcc34-opt'
    job.metadata          = 'evgen;%s;%s;%s' % (str({'FR': 46, 'NL': 45, 'NDGF': 300, 'CERN': 19, 'TW': 44110, 'CA': 2922, 'DE': 9903, 'IT': 1168, 'US': 6226, 'UK': 1026, 'ES': 26619}),str({999907:100,999906:200,999905:300}),str({999905:100,999906:910,999907:500}))
    #job.metadata          = 'evgen;%s' % str({'FR': 46, 'NL': 45, 'NDGF': 300, 'CERN': 19, 'TW': 44110, 'CA': 2922, 'DE': 9903, 'IT': 1168, 'US': 6226, 'UK': 1026, 'ES': 26619})

    #job.cloud = "UK"
    job.taskID = i
    
    file = FileSpec()
    file.lfn = "%s.evgen.pool.root" % job.jobName
    file.destinationDBlock = job.destinationDBlock
    file.destinationSE     = job.destinationSE
    file.dataset           = job.destinationDBlock
    #file.destinationDBlockToken = 'ATLASDATADISK'
    file.type = 'output'
    job.addFile(file)
    
    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)
    
    job.jobParameters="7087 0 500000 1 DC3.007087.singlepart_fwdgamma_etaplus_E500.py %s NONE NONE NONE" % file.lfn
    jobList.append(job)

for i in range(1):
    #s,o = Client.submitJobs(jobList)
    s,outS = Client.runTaskAssignment(jobList)    
    print "---------------------"
    print s
    for tmpOut in outS:
        print tmpOut
