import sys
import time
import uuid
import pandaserver.userinterface.Client as Client
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.FileSpec import FileSpec

if len(sys.argv)>1:
    site = sys.argv[1]
else:
    site = None

datasetName = 'panda.destDB.%s' % str(uuid.uuid4())
destName    = 'BNL_ATLAS_2'
#destName    = 'BU_ATLAS_Tier2'

files = {
    'mc11.007204.singlepart_mu4.evgen.EVNT.v11000302._00037.pool.root.1':None,
    'mc11.007204.singlepart_mu4.evgen.EVNT.v11000302._00038.pool.root.1':None,    
    }

jobList = []

for lfn in files:
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = str(uuid.uuid4())
    job.AtlasRelease      = 'Atlas-11.0.3'
    job.homepackage       = 'JobTransforms-11-00-03-02'
    job.transformation    = 'share/csc.simul.trf'
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.computingSite     = site
    job.prodDBlock        = 'mc11.007204.singlepart_mu4.evgen.EVNT.v11000302'
    job.cmtConfig         = 'i686-slc4-gcc34-opt'

    job.prodSourceLabel   = 'test'
    job.currentPriority   = 1000

    fileI = FileSpec()
    fileI.dataset    = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = 'input'
    job.addFile(fileI)

    fileOE = FileSpec()
    fileOE.lfn = "%s.HITS.pool.root" % str(uuid.uuid4())
    fileOE.destinationDBlock = job.destinationDBlock
    fileOE.destinationSE     = job.destinationSE
    fileOE.dataset           = job.destinationDBlock
    fileOE.destinationDBlockToken = 'ATLASDATADISK'
    fileOE.type = 'output'
    job.addFile(fileOE)

    fileOA = FileSpec()
    fileOA.lfn = "%s.RDO.pool.root" % str(uuid.uuid4())
    fileOA.destinationDBlock = job.destinationDBlock
    fileOA.destinationSE     = job.destinationSE
    fileOA.dataset           = job.destinationDBlock
    fileOA.destinationDBlockToken = 'ATLASDATADISK'    
    fileOA.type = 'output'
    job.addFile(fileOA)

    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % str(uuid.uuid4())
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.destinationDBlockToken = 'ATLASDATADISK'    
    fileOL.type = 'log'
    job.addFile(fileOL)

    job.jobParameters="%s %s %s  100 700 2158" % (fileI.lfn,fileOE.lfn,fileOA.lfn)

    jobList.append(job)
    
s,o = Client.submitJobs(jobList)
print("---------------------")
print(s)
for x in o:
    print("PandaID=%s" % x[0])
