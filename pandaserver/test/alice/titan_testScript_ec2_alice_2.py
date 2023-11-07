# titan_testScript_ec2_alice_2.py
# This script is for running A01_alicegeo through PanDA
#
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

site = sys.argv[1]

datasetName = f"panda.destDB.{str(uuid.uuid4())}"
destName = "local"

job = JobSpec()
job.jobDefinitionID = int(time.time()) % 10000
job.jobName = f"{str(uuid.uuid4())}"
# MPI transform on Titan that will run actual job
job.transformation = "/lustre/atlas/proj-shared/csc108/transforms/mpi_wrapper_alice_A01alicegeo.py"

job.destinationDBlock = datasetName
job.destinationSE = destName
job.currentPriority = 1000
job.prodSourceLabel = "panda"
job.computingSite = site
job.jobParameters = " "
job.VO = "alice"

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
