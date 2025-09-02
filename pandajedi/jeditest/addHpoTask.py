import uuid
from urllib.parse import quote

from userinterface import Client

taskParamMap = {}

taskParamMap["noInput"] = True
taskParamMap["nEventsPerJob"] = 1
taskParamMap["nEvents"] = 5
taskParamMap["taskName"] = str(uuid.uuid4())
taskParamMap["userName"] = "pandasrv1"
taskParamMap["vo"] = "atlas"
taskParamMap["taskPriority"] = 1000
taskParamMap["reqID"] = 12345
taskParamMap["architecture"] = ""
taskParamMap["hpoWorkflow"] = True
taskParamMap["transUses"] = ""
taskParamMap["transHome"] = ""
taskParamMap["transPath"] = "http://pandaserver.cern.ch:25080/trf/user/runHPO-00-00-01"
taskParamMap["processingType"] = "simul"
taskParamMap["prodSourceLabel"] = "test"
taskParamMap["useLocalIO"] = 1
taskParamMap["taskType"] = "prod"
taskParamMap["workingGroup"] = "AP_HPO"
taskParamMap["coreCount"] = 1
taskParamMap["site"] = "BNL_PROD_UCORE"
taskParamMap["nucleus"] = "CERN-PROD"
taskParamMap["cloud"] = "WORLD"

logDatasetName = f"panda.jeditest.log.{uuid.uuid4()}"
outDatasetName = f"panda.jeditest.HPO.{uuid.uuid4()}"

taskParamMap["log"] = {
    "dataset": logDatasetName,
    "type": "template",
    "param_type": "log",
    "token": "ddd:.*DATADISK",
    "destination": "(type=DATADISK)\(dontkeeplog=True)",
    "offset": 1000,
    "value": f"{logDatasetName}.${{SN}}.log.tgz",
}

taskParamMap["hpoRequestData"] = {
    "sandbox": "gitlab-registry.cern.ch/zhangruihpc/steeringcontainer:latest",
    "executable": "docker",
    "arguments": '/bin/bash -c "hpogrid generate --n_point=%NUM_POINTS ' "--max_point=%MAX_POINTS --infile=$PWD/%IN  --outfile=$PWD/%OUT " '-l=nevergrad"',
    "output_json": "output.json",
    "max_points": 10,
    "num_points_per_generation": 2,
}

taskParamMap["container_name"] = "docker://gitlab-registry.cern.ch/zhangruihpc/evaluationcontainer:mlflow"

taskParamMap["jobParameters"] = [
    {"type": "constant", "value": f"-o out.json -j \"\" -p \"{quote('bash ./exec_in_container.sh')}\""},
    {"type": "constant", "value": "--writeInputToTxt IN_DATA:input_ds.json --inSampleFile input_sample.json"},
    {"type": "constant", "value": "-a aaa.tgz --sourceURL https://aipanda048.cern.ch:25443"},
    {"type": "constant", "value": "--inMap \"{'IN_DATA': ${IN_DATA/T}}\""},
    {
        "type": "template",
        "param_type": "input",
        "value": '-i "${IN_DATA/T}"',
        "dataset": "mc16_13TeV.501103.MGPy8EG_StauStauDirect_220p0_1p0_TFilt.merge.EVNT.e8102_e7400_tid21342682_00",
        "attributes": "nosplit,repeat",
    },
    {
        "type": "template",
        "param_type": "output",
        "token": "ATLASDATADISK",
        "value": "$JEDITASKID.metrics.${SN}.tgz",
        "dataset": outDatasetName,
        "hidden": True,
    },
    {
        "type": "constant",
        "value": "--outMetricsFile=${OUTPUT0}^metrics.tgz",
    },
]

print(Client.insertTaskParams(taskParamMap))
