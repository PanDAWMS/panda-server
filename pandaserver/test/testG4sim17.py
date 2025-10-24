import sys
import time
import uuid

from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.userinterface import Client

site = sys.argv[1]
cloud = sys.argv[2]

prod_destination_block = "mc10_7TeV.105001.pythia_minbias.evgen.EVNT.e574_tid153937_00"
input_file = "EVNT.153937._000184.pool.root.1"

if len(sys.argv) == 5:
    site = sys.argv[1]
    cloud = sys.argv[2]
    prod_destination_block = sys.argv[3]
    input_file = sys.argv[4]

datasetName = f"panda.destDB.{str(uuid.uuid4())}"

files = {
    input_file: None,
}

job_list = []

index = 0
for lfn in files:
    index += 1
    job = JobSpec()
    job.jobDefinitionID = (time.time()) % 10000
    job.jobName = "%s_%d" % (str(uuid.uuid4()), index)
    job.AtlasRelease = "Atlas-17.0.5"
    job.homepackage = "AtlasProduction/17.0.5.6"
    job.transformation = "AtlasG4_trf.py"
    job.destinationDBlock = datasetName
    job.computingSite = site
    job.prodDBlock = prod_destination_block

    job.prodSourceLabel = "test"
    job.processingType = "test"
    job.currentPriority = 10000
    job.cloud = cloud
    job.cmtConfig = "i686-slc5-gcc43-opt"

    fileI = FileSpec()
    fileI.dataset = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = "input"
    job.addFile(fileI)

    fileD = FileSpec()
    fileD.dataset = "ddo.000001.Atlas.Ideal.DBRelease.v170602"
    fileD.prodDBlock = fileD.dataset
    fileD.lfn = "DBRelease-17.6.2.tar.gz"
    fileD.type = "input"
    job.addFile(fileD)

    fileOA = FileSpec()
    fileOA.lfn = f"{job.jobName}.HITS.pool.root"
    fileOA.destinationDBlock = job.destinationDBlock
    fileOA.destinationSE = job.destinationSE
    fileOA.dataset = job.destinationDBlock
    fileOA.destinationDBlockToken = "ATLASDATADISK"
    fileOA.type = "output"
    job.addFile(fileOA)

    fileOL = FileSpec()
    fileOL.lfn = f"{job.jobName}.job.log.tgz"
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE = job.destinationSE
    fileOL.dataset = job.destinationDBlock
    fileOL.destinationDBlockToken = "ATLASDATADISK"
    fileOL.type = "log"
    job.addFile(fileOL)
    job.jobParameters = (
        f"inputEvgenFile={fileI.lfn} outputHitsFile={fileOA.lfn} maxEvents=3 skipEvents=0 "
        f"DBRelease={fileD.lfn} geometryVersion=ATLAS-GEO-18-01-03_VALIDATION "
        f"conditionsTag=OFLCOND-SDR-BS7T-05-14 randomSeed=1 physicsList=QGSP_BERT "
        f"RunNumber=116870 firstEvent=1"
    )

    job_list.append(job)

status, output = Client.submit_jobs(job_list)
print("---------------------")
print(f"Status: {status}. Output: {output}")
