from ruamel import yaml
import logging
import sys
import copy

from pandaserver.workflow.workflow_utils import get_node_id_map, dump_nodes
from pandaserver.workflow.pcwl_utils import parse_workflow_file, resolve_nodes

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
with open(sys.argv[2]) as f:
    data = yaml.safe_load(f)
nodes, root_in = parse_workflow_file(sys.argv[1], logging)
s_id, t_nodes, nodes = resolve_nodes(nodes, root_in, data, 0, set(), sys.argv[3], logging)
id_map = get_node_id_map(nodes)
# task template
template = {"buildSpec": {"jobParameters": "-i ${IN} -o ${OUT} --sourceURL ${SURL} -r . ",
                               "archiveName": "sources.bfb28dae-cc83-4945-b110-486f0b9b9657.tar.gz",
                               "prodSourceLabel": "panda"},
                 "sourceURL": "https://aipanda059.cern.ch:25443",
                 "site": None,
                 "vo": "atlas",
                 "respectSplitRule": True,
                 "osInfo": "Linux-3.10.0-1160.25.1.el7.x86_64-x86_64-with-centos-7.9.2009-Core",
                 "transUses": "", "excludedSite": [],
                 "nMaxFilesPerJob": 200,
                 "uniqueTaskName": True,
                 "transHome": None,
                 "includedSite": None,
                 "container_name": "__dummy_container__",
                 "jobParameters": [{"type": "constant", "value": "-j \"\" --sourceURL ${SURL}"},
                                   {"type": "constant", "value": "-r ."},
                                   {"padding": False, "type": "constant", "value": "-p \""},
                                   {"padding": False, "type": "constant",
                                   "value": "__dummy_exec_str__"},
                                   {"type": "constant", "value": "\""},
                                   {"type": "constant", "value": "-l ${LIB}"}],
                 "prodSourceLabel": "user",
                 "processingType": "panda-client-1.4.79-jedi-run",
                 "multiStepExec": {"preprocess": {"args": "--preprocess ${TRF_ARGS}", "command": "${TRF}"},
                                   "containerOptions": {"containerImage": "__dummy_container__",
                                                        "containerExec": "echo \"=== cat exec script ===\"; "
                                                                         "cat __run_main_exec.sh; "
                                                                         "echo; echo \"=== exec script ===\"; "
                                                                         "/bin/sh __run_main_exec.sh"},
                                   "postprocess": {"args": "--postprocess ${TRF_ARGS}", "command": "${TRF}"}},
                 }

c_template = {"sourceURL": "https://aipanda048.cern.ch:25443",
              "cliParams": "prun --cwl test.cwl --yaml a.yaml "
                           "--relayHost aipanda059.cern.ch --outDS "
                           "user.tmaeno.1f2860f0-30d6-4352-9f87-9b9dde82fd80",
              "site": None,
              "vo": "atlas",
              "respectSplitRule": True,
              "osInfo": "Linux-3.10.0-1160.36.2.el7.x86_64-x86_64-with-centos-7.9.2009-Core",
              "log": {"type": "template",
                      "param_type": "log",
                      "container": "user.tmaeno.1f2860f0-30d6-4352-9f87-9b9dde82fd80.log/",
                      "value": "user.tmaeno.1f2860f0-30d6-4352-9f87-9b9dde82fd80.log.$JEDITASKID.${SN}.log.tgz",
                      "dataset": "user.tmaeno.1f2860f0-30d6-4352-9f87-9b9dde82fd80.log/"},
              "transUses": "",
              "excludedSite": [],
              "nMaxFilesPerJob": 200,
              "uniqueTaskName": True,
              "taskName": "user.tmaeno.1f2860f0-30d6-4352-9f87-9b9dde82fd80/",
              "transHome": None,
              "includedSite": None,
              "container_name": "__dummy_container__",
              "multiStepExec": {"preprocess": {"args": "--preprocess ${TRF_ARGS}", "command": "${TRF}"},
                                "containerOptions": {"containerImage": "__dummy_container__",
                                                     "containerExec": "echo \"=== cat exec script ===\"; "
                                                                      "cat __run_main_exec.sh; echo; "
                                                                      "echo \"=== exec script ===\"; "
                                                                      "/bin/sh __run_main_exec.sh"},
                                "postprocess": {"args": "--postprocess ${TRF_ARGS}", "command": "${TRF}"}},
              "jobParameters": [{"type": "constant", "value": "-j \"\" --sourceURL ${SURL}"},
                                {"type": "constant", "value": "-r ."},
                                {"padding": False, "type": "constant", "value": "-p \""},
                                {"padding": False, "type": "constant", "value": "__dummy_exec_str__"},
                                {"type": "constant", "value": "\""},
                                {"type": "constant", "value": "-a jobO.b6bdf294-8a46-4b02-b399-46619db4088b.tar.gz"}],
              "prodSourceLabel": "user",
              "processingType": "panda-client-1.4.81-jedi-run",
              "architecture": ""
              }

task_template = {'athena': copy.deepcopy(template), 'container': copy.deepcopy(c_template)}

[node.resolve_params(task_template, id_map) for node in nodes]
print(dump_nodes(nodes))

