import argparse
import json
import sys

from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import taskBuffer


def parse_args():
    parser = argparse.ArgumentParser(
        description="Workflow core smoke test helper",
        epilog=(
            "examples:\n"
            "  submit a description inline, with no sandbox:\n"
            "    %(prog)s submit_description --wfd-file pandaserver/workflow/examples/production_chain_wfd.json --prod-role\n"
            "  watch what the engine made of it:\n"
            "    %(prog)s show 12345\n"
            "  advance it one step by hand instead of waiting for the WatchDog:\n"
            "    %(prog)s process 12345\n"
            "  cancel it:\n"
            "    %(prog)s cancel_workflow 12345 --force\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "action",
        choices=["submit_description", "show", "process", "cancel_workflow"],
        help="Action to perform in the smoke test",
    )
    parser.add_argument("workflow_id", nargs="?", help="Workflow ID the action applies to; not used by submit_description")
    parser.add_argument("--force", action="store_true", help="Force into cancelled status")
    parser.add_argument("--wfd-file", help="Path to a JSON workflow description, for submit_description")
    parser.add_argument("--user-dn", default="/CN=smoketest", help="DN recorded as the submitter, for submit_description")
    parser.add_argument("--prod-role", action="store_true", help="Record the submitter as holding a production role, for submit_description")
    parser.add_argument("--repeat", type=int, default=1, help="How many times process should advance the workflow")
    args = parser.parse_args()
    if args.action == "submit_description":
        if not args.wfd_file:
            parser.error("submit_description needs --wfd-file")
    elif not args.workflow_id:
        parser.error(f"{args.action} needs a workflow_id")
    return args


def show_workflow(task_buffer, workflow_id):
    """Print what the engine currently holds for a workflow, its steps and its data"""
    workflow_spec = task_buffer.get_workflow(workflow_id=workflow_id)
    if workflow_spec is None:
        print(f"workflow_id={workflow_id} not found")
        return
    print(
        f"workflow_id={workflow_spec.workflow_id} name={workflow_spec.name} status={workflow_spec.status} "
        f"prodsourcelabel={workflow_spec.prodsourcelabel} user={workflow_spec.username}"
    )
    step_specs = task_buffer.get_steps_of_workflow(workflow_id=workflow_id) or []
    print(f"  steps ({len(step_specs)}):")
    for step_spec in sorted(step_specs, key=lambda s: s.member_id or 0):
        print(f"    [{step_spec.member_id}] {step_spec.name:<24} {step_spec.status:<12} flavor={step_spec.flavor:<12} target_id={step_spec.target_id}")
    data_specs = task_buffer.get_data_of_workflow(workflow_id=workflow_id) or []
    print(f"  data ({len(data_specs)}):")
    for data_spec in sorted(data_specs, key=lambda d: d.name or ""):
        # target_id still holding ${TASKID} means the producing task has not been queued yet
        print(f"    {data_spec.name:<28} {data_spec.status:<20} type={data_spec.type:<7} target_id={data_spec.target_id}")


# parameters for the workflow
# prodsourcelabel = "user"
# username = "testuser"
# workflow_name = "test_workflow_bg_comb_00"

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


def main():
    args = parse_args()
    WFID = args.workflow_id
    action = args.action
    force = args.force

    from pandaserver.workflow.workflow_core import WorkflowInterface

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
    # wf_spec = taskBuffer.get_workflow(workflow_id=WFID)
    # print("Processing registered workflow...")
    # wfif.process_workflow_registered(wf_spec)

    # wf_spec = taskBuffer.get_workflow(workflow_id=WFID)
    # print("Processing checked workflow...")
    # wfif.process_workflow_checked(wf_spec)

    # wf_spec = taskBuffer.get_workflow(workflow_id=WFID)
    # print("Processing starting workflow...")
    # wfif.process_workflow_starting(wf_spec)

    if args.action == "submit_description":
        # Register a workflow from a description given inline, the way the
        # submit_workflow_description API does. The description is stored as the raw request and
        # parsed asynchronously, so this returns as soon as the workflow row exists.
        with open(args.wfd_file) as wfd_file:
            workflow_description = json.load(wfd_file)
        workflow_data = workflow_description.get("workflow", workflow_description)
        raw_request_params = {"workflow_description": workflow_description}
        workflow_id = wfif.register_workflow(
            "managed" if args.prod_role else "user",
            args.user_dn,
            workflow_data.get("name"),
            raw_request_params=raw_request_params,
            prod_role=args.prod_role,
            fqans=[],
        )
        if workflow_id is None:
            print("Failed to register the workflow")
            return
        print(f"Registered workflow_id={workflow_id}")
        print(f"  next: {sys.argv[0]} process {workflow_id}")
    elif args.action == "show":
        show_workflow(taskBuffer, WFID)
    elif args.action == "process":
        # Advance the workflow by hand instead of waiting for the WatchDog cycle
        for attempt in range(args.repeat):
            workflow_spec = taskBuffer.get_workflow(workflow_id=WFID)
            if workflow_spec is None:
                print(f"workflow_id={WFID} not found")
                return
            before = workflow_spec.status
            process_result, workflow_spec = wfif.process_workflow(workflow_spec)
            print(f"  [{attempt + 1}] {before} -> {workflow_spec.status} " f"success={process_result.success} {process_result.message}")
            if before == workflow_spec.status:
                # nothing moved, so repeating will not help until something external changes
                break
        show_workflow(taskBuffer, WFID)
    elif args.action == "cancel_workflow":
        print(f"Cancelling workflow_id={WFID} ...")
        res = wfif.cancel_workflow(workflow_id=WFID, force=args.force)
        if res:
            print(f"Cancelled workflow_id={WFID} successfully.")
        else:
            print(f"Failed to cancel workflow_id={WFID}.")


if __name__ == "__main__":
    main()
