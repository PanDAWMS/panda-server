import sys
import time
import uuid

from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.userinterface import Client

if len(sys.argv) > 1:
    site = sys.argv[1]
else:
    site = None

dataset_name = f"panda.destDB.{str(uuid.uuid4())}"
destination_se = "BNL_ATLAS_2"
# destination_se    = 'BU_ATLAS_Tier2'

files = {
    "mc11.007204.singlepart_mu4.evgen.EVNT.v11000302._00037.pool.root.1": None,
    "mc11.007204.singlepart_mu4.evgen.EVNT.v11000302._00038.pool.root.1": None,
}

job_list = []

for lfn in files:
    job = JobSpec()
    job.jobDefinitionID = int(time.time()) % 10000
    job.jobName = str(uuid.uuid4())
    job.AtlasRelease = "Atlas-11.0.3"
    job.homepackage = "JobTransforms-11-00-03-02"
    job.transformation = "share/csc.simul.trf"
    job.destinationDBlock = dataset_name
    job.destinationSE = destination_se
    job.computingSite = site
    job.prodDBlock = "mc11.007204.singlepart_mu4.evgen.EVNT.v11000302"
    job.cmtConfig = "i686-slc4-gcc34-opt"

    job.prodSourceLabel = "test"
    job.currentPriority = 1000

    fileI = FileSpec()
    fileI.dataset = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = "input"
    job.addFile(fileI)

    fileOE = FileSpec()
    fileOE.lfn = f"{str(uuid.uuid4())}.HITS.pool.root"
    fileOE.destinationDBlock = job.destinationDBlock
    fileOE.destinationSE = job.destinationSE
    fileOE.dataset = job.destinationDBlock
    fileOE.destinationDBlockToken = "ATLASDATADISK"
    fileOE.type = "output"
    job.addFile(fileOE)

    fileOA = FileSpec()
    fileOA.lfn = f"{str(uuid.uuid4())}.RDO.pool.root"
    fileOA.destinationDBlock = job.destinationDBlock
    fileOA.destinationSE = job.destinationSE
    fileOA.dataset = job.destinationDBlock
    fileOA.destinationDBlockToken = "ATLASDATADISK"
    fileOA.type = "output"
    job.addFile(fileOA)

    fileOL = FileSpec()
    fileOL.lfn = f"{str(uuid.uuid4())}.job.log.tgz"
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE = job.destinationSE
    fileOL.dataset = job.destinationDBlock
    fileOL.destinationDBlockToken = "ATLASDATADISK"
    fileOL.type = "log"
    job.addFile(fileOL)

    job.jobParameters = f"{fileI.lfn} {fileOE.lfn} {fileOA.lfn}  100 700 2158"

    job_list.append(job)

status, output = Client.submit_jobs(job_list)
print("---------------------")
print(f"Status: {status}. Output: {output}")
