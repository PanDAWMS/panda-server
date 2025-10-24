import sys
import time
import uuid

from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.userinterface import Client

site = sys.argv[1]
cloud = sys.argv[2]

dataset_name = f"panda.destDB.{str(uuid.uuid4())}"
destination_se = None

n_jobs = 1
job_list = []

for i in range(n_jobs):
    job = JobSpec()
    job.jobDefinitionID = int(time.time()) % 10000
    job.jobName = "%s_%d" % (str(uuid.uuid4()), i)
    job.AtlasRelease = "Atlas-17.0.5"
    job.homepackage = "AtlasProduction/17.0.5.6"
    job.transformation = "Evgen_trf.py"
    job.destinationDBlock = dataset_name
    job.destinationSE = destination_se
    job.currentPriority = 10000
    job.prodSourceLabel = "test"
    job.computingSite = site
    job.cloud = cloud
    job.cmtConfig = "i686-slc5-gcc43-opt"

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

    job.jobParameters = f"7000 108316 1 5000 1 MC11.108316.Pythia8_minbias_ND.py {file.lfn}"

    job_list.append(job)

for i in range(n_jobs):
    status, output = Client.submit_jobs(job_list)
    print("---------------------")
    print(f"Status: {status}. Output: {output}")
