import sys
import time
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

site  = sys.argv[1]
cloud = sys.argv[2] 

datasetName = 'panda.destDB.%s' % commands.getoutput('uuidgen')
destName    = None

jobList = []

for i in range(1):
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = "%s_%d" % (commands.getoutput('uuidgen'),i)
    job.AtlasRelease      = 'Atlas-15.6.10'
    job.homepackage       = 'AtlasProduction/15.6.10.1'
    job.transformation    = 'Evgen_trf.py'
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.currentPriority   = 10000
    job.prodSourceLabel   = 'test'
    job.computingSite     = site
    job.cloud             = cloud
    job.cmtConfig         = 'i686-slc5-gcc43-opt'

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
    
    job.jobParameters="10000 105815 12330001 5000 12467 MC9.105815.JF140_pythia_jet_filter.py %s NONE NONE NONE MC09JobOpts-00-01-88.tar.gz" % file.lfn
    jobList.append(job)

for i in range(1):
    s,o = Client.submitJobs(jobList)
    print "---------------------"
    print s
    for x in o:
        print "PandaID=%s" % x[0]
