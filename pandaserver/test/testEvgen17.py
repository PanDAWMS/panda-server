import sys
import time
import uuid
import pandaserver.userinterface.Client as Client
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.FileSpec import FileSpec

site  = sys.argv[1]
cloud = sys.argv[2] 

datasetName = 'panda.destDB.%s' % str(uuid.uuid4())
destName    = None

jobList = []

for i in range(1):
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = "%s_%d" % (str(uuid.uuid4()),i)
    job.AtlasRelease      = 'Atlas-17.0.5'
    job.homepackage       = 'AtlasProduction/17.0.5.6'
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
    
    job.jobParameters="7000 108316 1 5000 1 MC11.108316.Pythia8_minbias_ND.py %s" % file.lfn
    
    jobList.append(job)

for i in range(1):
    s,o = Client.submitJobs(jobList)
    print("---------------------")
    print(s)
    for x in o:
        print("PandaID=%s" % x[0])
