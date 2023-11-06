import sys
import time
import uuid

import pandaserver.userinterface.Client as Client
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec

aSrvID = None

for idx, argv in enumerate(sys.argv):
    if argv == "-s":
        aSrvID = sys.argv[idx + 1]
        sys.argv = sys.argv[:idx]
        break

# site = sys.argv[1]
site = "ANALY_BNL-LSST"  # orig
# site = 'BNL-LSST'
# site = 'SWT2_CPB-LSST'
# site = 'UTA_SWT2-LSST'
# site = 'ANALY_SWT2_CPB-LSST'

datasetName = f"panda.user.jschovan.lsst.{str(uuid.uuid4())}"
destName = None

job = JobSpec()
job.jobDefinitionID = int(time.time()) % 10000
job.jobName = f"{str(uuid.uuid4())}"
# job.transformation    = 'http://www.usatlas.bnl.gov/~wenaus/lsst-trf/lsst-trf.sh'
# job.transformation    = 'http://pandawms.org/pandawms-jobcache/lsst-trf.sh'
job.transformation = "http://pandawms.org/pandawms-jobcache/lsst-trf-phosim332.sh"
job.destinationDBlock = datasetName
# job.destinationSE     = destName
job.destinationSE = "local"
job.currentPriority = 1000
# job.prodSourceLabel   = 'ptest'
# job.prodSourceLabel = 'panda'
# job.prodSourceLabel = 'ptest'
# job.prodSourceLabel = 'test'
# job.prodSourceLabel = 'ptest'
# 2014-01-27
# job.prodSourceLabel = 'user'
job.prodSourceLabel = "panda"
job.computingSite = site
job.jobParameters = ""
job.VO = "lsst"

fileOL = FileSpec()
fileOL.lfn = f"{job.jobName}.job.log.tgz"
fileOL.destinationDBlock = job.destinationDBlock
fileOL.destinationSE = job.destinationSE
fileOL.dataset = job.destinationDBlock
fileOL.type = "log"
job.addFile(fileOL)


s, o = Client.submitJobs([job], srvID=aSrvID)
print(s)
for x in o:
    print(f"PandaID={x[0]}")
