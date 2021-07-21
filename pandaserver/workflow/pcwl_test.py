from ruamel import yaml
import logging
import sys

from pandaserver.workflow.workflow_utils import get_node_id_map, dump_nodes
from pandaserver.workflow.pcwl_utils import parse_workflow_file, resolve_nodes

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
with open(sys.argv[2]) as f:
    data = yaml.safe_load(f)
nodes, root_in = parse_workflow_file(sys.argv[1], logging)
s_id, t_nodes, nodes = resolve_nodes(nodes, root_in, data, 0, set(), 'hogehoge', logging)
id_map = get_node_id_map(nodes)
# task template
task_template = {"buildSpec": {"jobParameters": "-i ${IN} -o ${OUT} --sourceURL ${SURL} -r . ",
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
[node.resolve_params(task_template, id_map) for node in nodes]
print(dump_nodes(nodes))

