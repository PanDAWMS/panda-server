import json
import sys

from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.workflow.workflow_core import (
    WFDataSpec,
    WFStepSpec,
    WorkflowInterface,
    WorkflowSpec,
)

# parameters for the workflow
prodsourcelabel = "user"
username = "testuser"
workflow_name = "test_workflow_bg_comb_00"

WFID = 1  # workflow ID to be used in this test

# workflow definition json
# wfd_json = json.dumps(
#     json.loads(
#         """
# {
#     "root_inputs": {
#             "sig_bg_comb.cwl#background": "mc16_5TeV.361238.Pythia8EvtGen_A3NNPDF23LO_minbias_inelastic_low.merge.HITS.e6446_s3238_s3250/",
#             "sig_bg_comb.cwl#signal": "mc16_valid:mc16_valid.900248.PG_singlepion_flatPt2to50.simul.HITS.e8312_s3238_tid26378578_00"
#         },
#     "root_outputs": {"sig_bg_comb.cwl#combine/outDS": {"value": "user.me.my_outDS_005_combine"}},
#     "nodes": [
#         {
#             "condition": null,
#             "data": null,
#             "id": 1,
#             "in_loop": false,
#             "inputs": {
#                 "sig_bg_comb.cwl#make_signal/opt_args": {
#                     "default": "--outputs abc.dat,def.zip --nFilesPerJob 5",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#make_signal/opt_containerImage": {
#                     "default": "docker://busybox",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#make_signal/opt_exec": {
#                     "default": "echo %IN > abc.dat; echo 123 > def.zip",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#make_signal/opt_inDS": {
#                     "default": null,
#                     "source": "sig_bg_comb.cwl#signal"
#                 }
#             },
#             "is_head": false,
#             "is_leaf": true,
#             "is_tail": false,
#             "is_workflow_output": false,
#             "loop": false,
#             "name": "make_signal",
#             "output_types": [],
#             "outputs": {
#                 "sig_bg_comb.cwl#make_signal/outDS": {}
#             },
#             "parents": [],
#             "root_inputs": null,
#             "scatter": null,
#             "sub_nodes": [],
#             "task_params": null,
#             "type": "prun",
#             "upper_root_inputs": null
#         },
#         {
#             "condition": null,
#             "data": null,
#             "id": 2,
#             "in_loop": false,
#             "inputs": {
#                 "sig_bg_comb.cwl#make_background_1/opt_args": {
#                     "default": "--outputs opq.root,xyz.pool --nGBPerJob 10",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#make_background_1/opt_exec": {
#                     "default": "echo %IN > opq.root; echo %IN > xyz.pool",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#make_background_1/opt_inDS": {
#                     "default": null,
#                     "source": "sig_bg_comb.cwl#background"
#                 }
#             },
#             "is_head": false,
#             "is_leaf": true,
#             "is_tail": false,
#             "is_workflow_output": false,
#             "loop": false,
#             "name": "make_background_1",
#             "output_types": [],
#             "outputs": {
#                 "sig_bg_comb.cwl#make_background_1/outDS": {}
#             },
#             "parents": [],
#             "root_inputs": null,
#             "scatter": null,
#             "sub_nodes": [],
#             "task_params": null,
#             "type": "prun",
#             "upper_root_inputs": null
#         },
#         {
#             "condition": null,
#             "data": null,
#             "id": 3,
#             "in_loop": false,
#             "inputs": {
#                 "sig_bg_comb.cwl#premix/opt_args": {
#                     "default": "--outputs klm.root --secondaryDSs IN2:2:%{SECDS1}",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#premix/opt_exec": {
#                     "default": "echo %IN %IN2 > klm.root",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#premix/opt_inDS": {
#                     "default": null,
#                     "parent_id": 1,
#                     "source": "sig_bg_comb.cwl#make_signal/outDS"
#                 },
#                 "sig_bg_comb.cwl#premix/opt_inDsType": {
#                     "default": "def.zip",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#premix/opt_secondaryDSs": {
#                     "default": null,
#                     "parent_id": [
#                         2
#                     ],
#                     "source": [
#                         "sig_bg_comb.cwl#make_background_1/outDS"
#                     ]
#                 },
#                 "sig_bg_comb.cwl#premix/opt_secondaryDsTypes": {
#                     "default": [
#                         "xyz.pool"
#                     ],
#                     "source": null
#                 }
#             },
#             "is_head": false,
#             "is_leaf": true,
#             "is_tail": false,
#             "is_workflow_output": false,
#             "loop": false,
#             "name": "premix",
#             "output_types": [],
#             "outputs": {
#                 "sig_bg_comb.cwl#premix/outDS": {}
#             },
#             "parents": [
#                 1,
#                 2
#             ],
#             "root_inputs": null,
#             "scatter": null,
#             "sub_nodes": [],
#             "task_params": null,
#             "type": "prun",
#             "upper_root_inputs": null
#         },
#         {
#             "condition": null,
#             "data": null,
#             "id": 4,
#             "in_loop": false,
#             "inputs": {
#                 "sig_bg_comb.cwl#generate_some/opt_args": {
#                     "default": "--outputs gen.root --nJobs 10",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#generate_some/opt_exec": {
#                     "default": "echo %RNDM:10 > gen.root",
#                     "source": null
#                 }
#             },
#             "is_head": false,
#             "is_leaf": true,
#             "is_tail": false,
#             "is_workflow_output": false,
#             "loop": false,
#             "name": "generate_some",
#             "output_types": [],
#             "outputs": {
#                 "sig_bg_comb.cwl#generate_some/outDS": {}
#             },
#             "parents": [],
#             "root_inputs": null,
#             "scatter": null,
#             "sub_nodes": [],
#             "task_params": null,
#             "type": "prun",
#             "upper_root_inputs": null
#         },
#         {
#             "condition": null,
#             "data": null,
#             "id": 5,
#             "in_loop": false,
#             "inputs": {
#                 "sig_bg_comb.cwl#make_background_2/opt_args": {
#                     "default": "--outputs ooo.root,jjj.txt --secondaryDSs IN2:2:%{SECDS1}",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#make_background_2/opt_containerImage": {
#                     "default": "docker://alpine",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#make_background_2/opt_exec": {
#                     "default": "echo %IN > ooo.root; echo %IN2 > jjj.txt",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#make_background_2/opt_inDS": {
#                     "default": null,
#                     "source": "sig_bg_comb.cwl#background"
#                 },
#                 "sig_bg_comb.cwl#make_background_2/opt_secondaryDSs": {
#                     "default": null,
#                     "parent_id": [
#                         4
#                     ],
#                     "source": [
#                         "sig_bg_comb.cwl#generate_some/outDS"
#                     ]
#                 },
#                 "sig_bg_comb.cwl#make_background_2/opt_secondaryDsTypes": {
#                     "default": [
#                         "gen.root"
#                     ],
#                     "source": null
#                 }
#             },
#             "is_head": false,
#             "is_leaf": true,
#             "is_tail": false,
#             "is_workflow_output": false,
#             "loop": false,
#             "name": "make_background_2",
#             "output_types": [],
#             "outputs": {
#                 "sig_bg_comb.cwl#make_background_2/outDS": {}
#             },
#             "parents": [
#                 4
#             ],
#             "root_inputs": null,
#             "scatter": null,
#             "sub_nodes": [],
#             "task_params": null,
#             "type": "prun",
#             "upper_root_inputs": null
#         },
#         {
#             "condition": null,
#             "data": null,
#             "id": 6,
#             "in_loop": false,
#             "inputs": {
#                 "sig_bg_comb.cwl#combine/opt_args": {
#                     "default": "--outputs aaa.root --secondaryDSs IN2:2:%{SECDS1},IN3:5:%{SECDS2}",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#combine/opt_exec": {
#                     "default": "echo %IN %IN2 %IN3 > aaa.root",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#combine/opt_inDS": {
#                     "default": null,
#                     "parent_id": 1,
#                     "source": "sig_bg_comb.cwl#make_signal/outDS"
#                 },
#                 "sig_bg_comb.cwl#combine/opt_inDsType": {
#                     "default": "abc.dat",
#                     "source": null
#                 },
#                 "sig_bg_comb.cwl#combine/opt_secondaryDSs": {
#                     "default": null,
#                     "parent_id": [
#                         3,
#                         5
#                     ],
#                     "source": [
#                         "sig_bg_comb.cwl#premix/outDS",
#                         "sig_bg_comb.cwl#make_background_2/outDS"
#                     ]
#                 },
#                 "sig_bg_comb.cwl#combine/opt_secondaryDsTypes": {
#                     "default": [
#                         "klm.root",
#                         "ooo.root"
#                     ],
#                     "source": null
#                 }
#             },
#             "is_head": false,
#             "is_leaf": true,
#             "is_tail": true,
#             "is_workflow_output": false,
#             "loop": false,
#             "name": "combine",
#             "output_types": [],
#             "outputs": {
#                 "sig_bg_comb.cwl#combine/outDS": {}
#             },
#             "parents": [
#                 1,
#                 3,
#                 5
#             ],
#             "root_inputs": null,
#             "scatter": null,
#             "sub_nodes": [],
#             "task_params": null,
#             "type": "prun",
#             "upper_root_inputs": null
#         }
#     ]
# }
# """
#     )
# )


# interface for workflow operations
requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
taskBuffer.init(
    panda_config.dbhost,
    panda_config.dbpasswd,
    nDBConnection=panda_config.nDBConnection,
    useTimeout=True,
    requester=requester_id,
)

wfif = WorkflowInterface(taskBuffer)


# Test cases for workflow core

# Register the workflow
# print("Registering workflow...")
# wf_spec = wfif.register_workflow(
#     prodsourcelabel=prodsourcelabel,
#     username=username,
#     workflow_name=workflow_name,
#     workflow_definition_json=wfd_json,
# )


# Process the registered workflow
wf_spec = taskBuffer.get_workflow(workflow_id=WFID)
print("Processing registered workflow...")
wfif.process_workflow_registered(wf_spec)

wf_spec = taskBuffer.get_workflow(workflow_id=WFID)
print("Processing parsed workflow...")
wfif.process_workflow_parsed(wf_spec)
