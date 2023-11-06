import logging
import sys

from pandaserver.workflow.pcwl_utils import parse_workflow_file, resolve_nodes
from pandaserver.workflow.workflow_utils import (
    convert_nodes_to_workflow,
    dump_nodes,
    get_node_id_map,
    set_workflow_outputs,
)
from ruamel import yaml

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG)
with open(sys.argv[2]) as f:
    data = yaml.safe_load(f) or dict()
nodes, root_in = parse_workflow_file(sys.argv[1], logging)
s_id, t_nodes, nodes = resolve_nodes(nodes, root_in, data, 0, set(), sys.argv[3], logging)
set_workflow_outputs(nodes)
id_map = get_node_id_map(nodes)
# task template
template = {
    "buildSpec": {
        "jobParameters": "-i ${IN} -o ${OUT} --sourceURL ${SURL} " "-r ./ --useAthenaPackages --useCMake --cmtConfig x86_64-slc6-gcc49-opt ",
        "archiveName": "sources.5bca61db-3b0d-4e65-b102-7d56d82f4567.tar.gz",
        "prodSourceLabel": "panda",
    },
    "sourceURL": "https://aipanda047.cern.ch:25443",
    "cliParams": "prun --exec ls --useAthenaPackages --outDS user.tmaeno.4586bbc2-e891-441b-a9f4-4a9ceb9a9f8d",
    "site": None,
    "vo": "atlas",
    "respectSplitRule": True,
    "osInfo": "Linux-3.10.0-1160.36.2.el7.x86_64-x86_64-with-centos-7.9.2009-Core",
    "log": {
        "type": "template",
        "param_type": "log",
        "container": "user.tmaeno.4586bbc2-e891-441b-a9f4-4a9ceb9a9f8d.log/",
        "value": "user.tmaeno.4586bbc2-e891-441b-a9f4-4a9ceb9a9f8d.log.$JEDITASKID.${SN}.log.tgz",
        "dataset": "user.tmaeno.4586bbc2-e891-441b-a9f4-4a9ceb9a9f8d.log/",
    },
    "transUses": "Atlas-21.0.6",
    "excludedSite": [],
    "nMaxFilesPerJob": 200,
    "uniqueTaskName": True,
    "taskName": "user.tmaeno.4586bbc2-e891-441b-a9f4-4a9ceb9a9f8d/",
    "transHome": "AnalysisTransforms",
    "includedSite": None,
    "jobParameters": [
        {"type": "constant", "value": '-j "" --sourceURL ${SURL}'},
        {"type": "constant", "value": "-r ./"},
        {"padding": False, "type": "constant", "value": '-p "'},
        {"padding": False, "type": "constant", "value": "ls"},
        {"type": "constant", "value": '"'},
        {"type": "constant", "value": "-l ${LIB}"},
        {
            "type": "constant",
            "value": "--useAthenaPackages " "--useCMake --cmtConfig x86_64-slc6-gcc49-opt ",
        },
    ],
    "prodSourceLabel": "user",
    "processingType": "panda-client-1.4.81-jedi-run",
    "architecture": "x86_64-slc6-gcc49-opt@centos7",
}


c_template = {
    "sourceURL": "https://aipanda048.cern.ch:25443",
    "cliParams": "prun --cwl test.cwl --yaml a.yaml " "--relayHost aipanda059.cern.ch --outDS " "user.tmaeno.1f2860f0-30d6-4352-9f87-9b9dde82fd80",
    "site": None,
    "vo": "atlas",
    "respectSplitRule": True,
    "osInfo": "Linux-3.10.0-1160.36.2.el7.x86_64-x86_64-with-centos-7.9.2009-Core",
    "log": {
        "type": "template",
        "param_type": "log",
        "container": "user.tmaeno.1f2860f0-30d6-4352-9f87-9b9dde82fd80.log/",
        "value": "user.tmaeno.1f2860f0-30d6-4352-9f87-9b9dde82fd80.log.$JEDITASKID.${SN}.log.tgz",
        "dataset": "user.tmaeno.1f2860f0-30d6-4352-9f87-9b9dde82fd80.log/",
    },
    "transUses": "",
    "excludedSite": [],
    "nMaxFilesPerJob": 200,
    "uniqueTaskName": True,
    "taskName": "user.tmaeno.1f2860f0-30d6-4352-9f87-9b9dde82fd80/",
    "transHome": None,
    "includedSite": None,
    "container_name": "__dummy_container__",
    "multiStepExec": {
        "preprocess": {"args": "--preprocess ${TRF_ARGS}", "command": "${TRF}"},
        "containerOptions": {
            "containerImage": "__dummy_container__",
            "containerExec": 'echo "=== cat exec script ==="; ' "cat __run_main_exec.sh; echo; " 'echo "=== exec script ==="; ' "/bin/sh __run_main_exec.sh",
        },
        "postprocess": {"args": "--postprocess ${TRF_ARGS}", "command": "${TRF}"},
    },
    "jobParameters": [
        {"type": "constant", "value": '-j "" --sourceURL ${SURL}'},
        {"type": "constant", "value": "-r ."},
        {"padding": False, "type": "constant", "value": '-p "'},
        {"padding": False, "type": "constant", "value": "__dummy_exec_str__"},
        {"type": "constant", "value": '"'},
        {
            "type": "constant",
            "value": "-a jobO.b6bdf294-8a46-4b02-b399-46619db4088b.tar.gz",
        },
    ],
    "prodSourceLabel": "user",
    "processingType": "panda-client-1.4.81-jedi-run",
    "architecture": "",
}

task_template = {"athena": template, "container": c_template}

[node.resolve_params(task_template, id_map) for node in nodes]
print(dump_nodes(nodes))

workflow, dump_str_list = convert_nodes_to_workflow(nodes)
print("".join(dump_str_list))

for node in nodes:
    s, o = node.verify()
    if not s:
        print(f"Verification error in ID:{node.id} {o}")
