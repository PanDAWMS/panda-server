import sys
import time
import uuid

import pandaserver.userinterface.Client as Client
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec

if len(sys.argv) > 1:
    site = sys.argv[1]
    cloud = None
else:
    site = None
    cloud = "US"


# cloud = 'TW'
# Recent changes (BNL migration to LFC?) forvce the cloud to be specified
cloud = "US"

datasetName = f"panda.destDB.{str(uuid.uuid4())}"
destName = "BNL_ATLAS_2"

files = {
    "EVNT.023986._00001.pool.root.1": None,
    # 'EVNT.023989._00001.pool.root.1':None,
}

jobList = []

index = 0
for lfn in files:
    index += 1
    job = JobSpec()
    job.jobDefinitionID = (time.time()) % 10000
    job.jobName = "%s_%d" % (str(uuid.uuid4()), index)
    job.AtlasRelease = "Atlas-14.2.20"
    job.homepackage = "AtlasProduction/14.2.20.1"
    job.transformation = "csc_simul_reco_trf.py"
    job.destinationDBlock = datasetName
    job.destinationSE = destName
    job.computingSite = site
    job.prodDBlock = "mc08.105031.Jimmy_jetsJ2.evgen.EVNT.e347_tid023986"
    # job.prodDBlock        = 'mc08.105034.Jimmy_jetsJ5.evgen.EVNT.e347_tid023989'

    job.prodSourceLabel = "test"
    job.processingType = "test"
    job.currentPriority = 10000
    job.cloud = cloud

    fileI = FileSpec()
    fileI.dataset = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = "input"
    job.addFile(fileI)

    fileD = FileSpec()
    fileD.dataset = "ddo.000001.Atlas.Ideal.DBRelease.v050601"
    fileD.prodDBlock = "ddo.000001.Atlas.Ideal.DBRelease.v050601"
    fileD.lfn = "DBRelease-5.6.1.tar.gz"
    fileD.type = "input"
    job.addFile(fileD)

    fileOA = FileSpec()
    fileOA.lfn = f"{job.jobName}.AOD.pool.root"
    fileOA.destinationDBlock = job.destinationDBlock
    fileOA.destinationSE = job.destinationSE
    fileOA.dataset = job.destinationDBlock
    fileOA.destinationDBlockToken = "ATLASDATADISK"
    fileOA.type = "output"
    job.addFile(fileOA)

    fileOE = FileSpec()
    fileOE.lfn = f"{job.jobName}.ESD.pool.root"
    fileOE.destinationDBlock = job.destinationDBlock
    fileOE.destinationSE = job.destinationSE
    fileOE.dataset = job.destinationDBlock
    fileOE.destinationDBlockToken = "ATLASDATADISK"
    fileOE.type = "output"
    job.addFile(fileOE)

    fileOL = FileSpec()
    fileOL.lfn = f"{job.jobName}.job.log.tgz"
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE = job.destinationSE
    fileOL.dataset = job.destinationDBlock
    fileOL.destinationDBlockToken = "ATLASDATADISK"
    fileOL.type = "log"
    job.addFile(fileOL)

    job.jobParameters = (
        "%s %s 30 500 3 ATLAS-GEO-02-01-00 3 3 QGSP_BERT jobConfig.VertexPosFastIDKiller.py FastSimulationJobTransforms/FastCaloSimAddCellsRecConfig.py,NoTrackSlimming.py %s OFF NONE NONE %s NONE"
        % (fileI.lfn, fileOA.lfn, fileD.lfn, fileOE.lfn)
    )

    jobList.append(job)

s, o = Client.submitJobs(jobList)
print("---------------------")
print(s)
for x in o:
    print(f"PandaID={x[0]}")
