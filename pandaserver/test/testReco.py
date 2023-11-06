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

files = {
    "misal1_mc12.005802.JF17_pythia_jet_filter.digit.RDO.v12000601_tid008610._11615.pool.root.1": None,
    # 'misal1_mc12.005802.JF17_pythia_jet_filter.digit.RDO.v12000601_tid008610._11639.pool.root.1':None,
    # 'misal1_mc12.005200.T1_McAtNlo_Jimmy.digit.RDO.v12000601_tid007554._03634.pool.root.1':None,
    # 'misal1_mc12.005200.T1_McAtNlo_Jimmy.digit.RDO.v12000601_tid007554._03248.pool.root.1':None,
    # 'misal1_mc12.005200.T1_McAtNlo_Jimmy.digit.RDO.v12000601_tid007554._03634.pool.root.1':None,
}

jobList = []

index = 0
for lfn in files:
    index += 1
    job = JobSpec()
    job.jobDefinitionID = int(time.time()) % 10000
    job.jobName = "%s_%d" % (str(uuid.uuid4()), index)
    job.AtlasRelease = "Atlas-12.0.6"
    job.homepackage = "AtlasProduction/12.0.6.4"
    job.transformation = "csc_reco_trf.py"
    job.destinationDBlock = datasetName
    job.destinationSE = destName
    job.computingSite = site
    # job.prodDBlock        = 'misal1_mc12.005200.T1_McAtNlo_Jimmy.digit.RDO.v12000601_tid007554'
    job.prodDBlock = "misal1_mc12.005802.JF17_pythia_jet_filter.digit.RDO.v12000601_tid008610"
    job.cloud = "US"

    job.prodSourceLabel = "test"
    job.currentPriority = 10000
    job.cmtConfig = "i686-slc4-gcc34-opt"

    fileI = FileSpec()
    fileI.dataset = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = "input"
    job.addFile(fileI)

    fileD = FileSpec()
    fileD.dataset = "ddo.000001.Atlas.Ideal.DBRelease.v030101"
    fileD.prodDBlock = "ddo.000001.Atlas.Ideal.DBRelease.v030101"
    fileD.lfn = "DBRelease-3.1.1.tar.gz"
    fileD.type = "input"
    job.addFile(fileD)

    fileOE = FileSpec()
    fileOE.lfn = f"{job.jobName}.ESD.pool.root"
    fileOE.destinationDBlock = job.destinationDBlock
    fileOE.destinationSE = job.destinationSE
    fileOE.dataset = job.destinationDBlock
    fileOE.destinationDBlockToken = "ATLASDATADISK"
    fileOE.type = "output"
    job.addFile(fileOE)

    fileOA = FileSpec()
    fileOA.lfn = f"{job.jobName}.AOD.pool.root"
    fileOA.destinationDBlock = job.destinationDBlock
    fileOA.destinationSE = job.destinationSE
    fileOA.dataset = job.destinationDBlock
    fileOA.destinationDBlockToken = "ATLASDATADISK"
    fileOA.type = "output"
    job.addFile(fileOA)

    fileOC = FileSpec()
    fileOC.lfn = f"{job.jobName}.NTUP.root"
    fileOC.destinationDBlock = job.destinationDBlock
    fileOC.destinationSE = job.destinationSE
    fileOC.dataset = job.destinationDBlock
    fileOC.destinationDBlockToken = "ATLASDATADISK"
    fileOC.type = "output"
    job.addFile(fileOC)

    fileOL = FileSpec()
    fileOL.lfn = f"{job.jobName}.job.log.tgz"
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE = job.destinationSE
    fileOL.dataset = job.destinationDBlock
    fileOL.destinationDBlockToken = "ATLASDATADISK"
    fileOL.type = "log"
    job.addFile(fileOL)

    job.jobParameters = f"{fileI.lfn} {fileOE.lfn} {fileOA.lfn} {fileOC.lfn} 250 0 ATLAS-CSC-01-02-00 CSC-06 NoRestrictedESDRecConfig.py {fileD.lfn}"

    jobList.append(job)

s, o = Client.submitJobs(jobList)
print("---------------------")
print(s)
for x in o:
    print(f"PandaID={x[0]}")
