import sys
import uuid

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandaserver.taskbuffer.JediDatasetSpec import JediDatasetSpec
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

task = JediTaskSpec()
task.jediTaskID = sys.argv[1]
task.taskName = f"pandatest.{uuid.uuid4()}"
task.status = "defined"
task.userName = "pandasrv1"
task.vo = "atlas"
task.prodSourceLabel = "managed"
task.taskPriority = 100
task.currentPriority = task.taskPriority
task.architecture = "i686-slc5-gcc43-opt"
task.transUses = "Atlas-17.2.7"
task.transHome = "AtlasProduction-17.2.8.10"
task.transPath = "Reco_trf.py"
task.workQueue_ID = 3

tbIF.insertTask_JEDI(task)


ds = JediDatasetSpec()
ds.jediTaskID = task.jediTaskID
if len(sys.argv) > 2:
    ds.datasetName = sys.argv[2]
else:
    ds.datasetName = "data12_8TeV.00214651.physics_Egamma.merge.AOD.f489_m1261"
ds.type = "input"
ds.vo = task.vo
ds.cloud = "US"
ds.streamName = "IN"
ds.status = "defined"
ds.nFiles = 0
ds.nFilesUsed = 0
ds.nFilesFinished = 0
ds.nFilesFailed = 0

st, datasetID = tbIF.insertDataset_JEDI(ds)

ds = JediDatasetSpec()
ds.jediTaskID = task.jediTaskID
ds.datasetName = "ddo.000001.Atlas.Ideal.DBRelease.v220701"
ds.type = "input"
ds.vo = task.vo
ds.cloud = "US"
ds.streamName = "DBR"
ds.status = "defined"
ds.nFiles = 0
ds.nFilesUsed = 0
ds.nFilesFinished = 0
ds.nFilesFailed = 0
ds.masterID = datasetID
ds.attributes = "repeat,nosplit"

tbIF.insertDataset_JEDI(ds)


ds = JediDatasetSpec()
ds.jediTaskID = task.jediTaskID
ds.datasetName = f"panda.jeditest.GEN.{uuid.uuid4()}"
ds.type = "output"
ds.vo = task.vo
ds.cloud = "US"
ds.streamName = "OUT"
ds.status = "defined"
ds.nFiles = 0
ds.nFilesUsed = 0
ds.nFilesFinished = 0
ds.nFilesFailed = 0

st, datasetID = tbIF.insertDataset_JEDI(ds)

tbIF.insertOutputTemplate_JEDI(
    [
        {
            "jediTaskID": task.jediTaskID,
            "datasetID": datasetID,
            "filenameTemplate": f"{ds.datasetName}.${{SN}}.pool.root",
            "serialNr": 1,
            "streamName": "OUT",
            "outtype": ds.type,
        }
    ]
)

ds = JediDatasetSpec()
ds.jediTaskID = task.jediTaskID
ds.datasetName = f"panda.jeditest.log.{uuid.uuid4()}"
ds.type = "log"
ds.vo = task.vo
ds.cloud = "US"
ds.streamName = "LOG"
ds.status = "defined"
ds.nFiles = 0
ds.nFilesUsed = 0
ds.nFilesFinished = 0
ds.nFilesFailed = 0

st, datasetID = tbIF.insertDataset_JEDI(ds)

tbIF.insertOutputTemplate_JEDI(
    [
        {
            "jediTaskID": task.jediTaskID,
            "datasetID": datasetID,
            "filenameTemplate": f"{ds.datasetName}.${{SN}}.log.tgz",
            "serialNr": 1,
            "streamName": "LOG",
            "outtype": ds.type,
        }
    ]
)

tbIF.insertJobParamsTemplate_JEDI(
    task.jediTaskID,
    'inputAODFile=${IN} maxEvents=1000 RunNumber=213816 autoConfiguration=everything preExec="from BTagging.BTaggingFlags import BTaggingFlags;BTaggingFlags.CalibrationTag="BTagCalibALL-07-02"" DBRelease=${DBR} AMITag=p1462 outputNTUP_EMBLLDNFile=${OUT}',
)
