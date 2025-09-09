import sys

try:
    metaID = sys.argv[1]
except Exception:
    metaID = None
import json
import uuid

taskParamMap = {}
taskParamMap["taskName"] = str(uuid.uuid4())
taskParamMap["userName"] = "pandasrv1"
taskParamMap["vo"] = "atlas"
taskParamMap["taskPriority"] = 100
taskParamMap["architecture"] = "i686-slc5-gcc43-opt"
taskParamMap["transUses"] = "Atlas-17.2.7"
taskParamMap["transHome"] = "AtlasProduction-17.2.8.10"
taskParamMap["transPath"] = "Reco_trf.py"
taskParamMap["processingType"] = "reco"
taskParamMap["prodSourceLabel"] = "test"
taskParamMap["taskType"] = "prod"
taskParamMap["workingGroup"] = "AP_Higgs"
taskParamMap["coreCount"] = 1
taskParamMap["cloud"] = "US"
logDatasetName = f"panda.jeditest.log.{uuid.uuid4()}"
taskParamMap["log"] = {
    "dataset": logDatasetName,
    "type": "template",
    "param_type": "log",
    "token": "ATLASDATADISK",
    "value": f"{logDatasetName}.${{SN}}.log.tgz",
}
outDatasetName = f"panda.jeditest.NTUP_EMBLLDN.{uuid.uuid4()}"
taskParamMap["jobParameters"] = [
    {
        "type": "template",
        "param_type": "input",
        "value": "inputAODFile=${IN}",
        "dataset": "data12_8TeV.00214651.physics_Egamma.merge.AOD.f489_m1261",
    },
    {
        "type": "constant",
        "value": 'maxEvents=1000 RunNumber=213816 autoConfiguration=everything preExec="from BTagging.BTaggingFlags import BTaggingFlags;BTaggingFlags.CalibrationTag="BTagCalibALL-07-02""',
    },
    {
        "type": "template",
        "param_type": "input",
        "value": "DBRelease=${DBR}",
        "dataset": "ddo.000001.Atlas.Ideal.DBRelease.v220701",
        "attributes": "repeat,nosplit",
    },
    {"type": "constant", "value": "AMITag=p1462"},
    {
        "type": "template",
        "param_type": "output",
        "token": "ATLASDATADISK",
        "value": f"outputNTUP_EMBLLDNFile={outDatasetName}.${{SN}}.pool.root",
        "dataset": outDatasetName,
    },
]

jonStr = json.dumps(taskParamMap)

from pandajedi.jedicore.JediTaskBufferInterface import (  # noqa: E402
    JediTaskBufferInterface,
)

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()
tbIF.insertTaskParams_JEDI(taskParamMap["vo"], taskParamMap["prodSourceLabel"], taskParamMap["userName"], taskParamMap["taskName"], jonStr)
