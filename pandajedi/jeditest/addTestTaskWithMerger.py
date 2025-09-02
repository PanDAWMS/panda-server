import sys

try:
    metaID = sys.argv[1]
except Exception:
    metaID = None
import json
import uuid

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

tbIF = JediTaskBufferInterface()
tbIF.setupInterface()

reqIdx = 100000000
dsName = "mc12_8TeV.129933.McAtNloJimmy_AUET2CT10_WW_e_e_0_0_0_0_0.merge.AOD.e1563_s1499_s1504_r3658_r3549_tid00999168_00"
logDatasetName = f"panda.jeditest.log.{uuid.uuid4()}"

taskParamMap = {}

taskParamMap["nFilesPerJob"] = 1
taskParamMap["nFiles"] = 2
taskParamMap["nEventsPerInputFile"] = 10000
taskParamMap["nEventsPerJob"] = 10000
# taskParamMap['nEvents'] = 25000
# taskParamMap['skipScout'] = True
taskParamMap["taskName"] = str(uuid.uuid4())
taskParamMap["userName"] = "pandasrv1"
taskParamMap["vo"] = "atlas"
taskParamMap["taskPriority"] = 900
# taskParamMap['reqID'] = reqIdx
taskParamMap["architecture"] = "i686-slc5-gcc43-opt"
taskParamMap["transUses"] = "Atlas-17.2.2"
taskParamMap["transHome"] = "AtlasProduction-17.2.2.2"
taskParamMap["transPath"] = "Generate_trf.py"
taskParamMap["processingType"] = "evgen"
taskParamMap["prodSourceLabel"] = "test"
taskParamMap["taskType"] = "prod"
taskParamMap["workingGroup"] = "AP_Higgs"
# taskParamMap['coreCount'] = 1
# taskParamMap['walltime'] = 1
taskParamMap["cloud"] = "FR"
# taskParamMap['site'] = 'FZK-LCG2'
taskParamMap["log"] = {
    "dataset": logDatasetName,
    "type": "template",
    "param_type": "log",
    "token": "ATLASDATADISK",
    "offset": 1000,
    "value": f"{logDatasetName}.${{SN}}.log.tgz",
}
outDatasetName = f"panda.jeditest.EVNT.{uuid.uuid4()}"


taskParamMap["jobParameters"] = [
    {
        "type": "template",
        "param_type": "input",
        "value": "inputGenConfFile=${IN}",
        "dataset": dsName,
    },
    {"type": "constant", "value": "ecmEnergy=8000 runNumber=12345"},
    {
        "type": "template",
        "value": "maxEvents=${MAXEVENTS}",
        "param_type": "number",
    },
    {"type": "template", "value": "randomSeed=${RNDMSEED}", "param_type": "number", "offset": 1000},
    {
        "type": "constant",
        "value": "jobConfig=MC12JobOptions/MC12.161559.PowHegPythia8_AU2CT10_ggH145_tautaull.py",
    },
    {
        "type": "template",
        "param_type": "output",
        "token": "ATLASDATADISK",
        "value": f"outputEVNTFile={outDatasetName}.${{SN}}.pool.root",
        "dataset": outDatasetName,
        "offset": 1000,
    },
    {
        "type": "constant",
        "value": "evgenJobOpts=MC12JobOpts-00-01-06_v1.tar.gz",
    },
]

# params to merge output
taskParamMap["mergeOutput"] = True
taskParamMap["mergeSpec"] = {}
taskParamMap["mergeSpec"]["transPath"] = "HITSMerge_tf.py"
taskParamMap["mergeSpec"][
    "jobParameters"
] = "--AMITag s1776 --DBRelease=current --autoConfiguration=everything --outputHitsFile=${OUTPUT0} --inputHitsFile=${TRN_OUTPUT0} --inputLogsFile=${TRN_LOG0}"

jonStr = json.dumps(taskParamMap)

tbIF.insertTaskParams_JEDI(taskParamMap["vo"], taskParamMap["prodSourceLabel"], taskParamMap["userName"], taskParamMap["taskName"], jonStr)
