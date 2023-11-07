import sys
import time
import uuid

import pandaserver.userinterface.Client as Client
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec

if len(sys.argv) > 1:
    site = sys.argv[1]
else:
    site = None

datasetName = f"panda.destDB.{str(uuid.uuid4())}"
destName = None

jobList = []

for i in range(1):
    job = JobSpec()
    job.jobDefinitionID = int(time.time()) % 10000
    job.jobName = "%s_%d" % (str(uuid.uuid4()), i)
    job.AtlasRelease = "Atlas-14.1.0"
    job.homepackage = "AtlasProduction/14.1.0.3"
    job.transformation = "csc_evgen_trf.py"
    job.destinationDBlock = datasetName
    job.destinationSE = destName
    job.currentPriority = 100
    job.prodSourceLabel = "test"
    job.computingSite = site
    job.cloud = "US"
    job.cmtConfig = "i686-slc4-gcc34-opt"

    file = FileSpec()
    file.lfn = f"{job.jobName}.evgen.pool.root"
    file.destinationDBlock = job.destinationDBlock
    file.destinationSE = job.destinationSE
    file.dataset = job.destinationDBlock
    file.destinationDBlockToken = "ATLASDATADISK"
    file.type = "output"
    job.addFile(file)

    fileOL = FileSpec()
    fileOL.lfn = f"{job.jobName}.job.log.tgz"
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE = job.destinationSE
    fileOL.dataset = job.destinationDBlock
    fileOL.destinationDBlockToken = "ATLASDATADISK"
    fileOL.type = "log"
    job.addFile(fileOL)

    job.jobParameters = f"5144 1 5000 1 CSC.005144.PythiaZee.py {file.lfn} NONE NONE NONE"
    jobList.append(job)

for i in range(1):
    s, o = Client.submitJobs(jobList)
    print("---------------------")
    print(s)
    for x in o:
        print(f"PandaID={x[0]}")
