import argparse
import json
import sys

from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.workflow.workflow_base import WFDataType


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
    parser.add_argument(
        "--user-dn",
        default="panda",
        help=(
            "DN recorded as the submitter, for submit_description. Note that this is passed to "
            "checkBanUser with jediCheck on, which creates the user if the name is unknown and "
            "otherwise overwrites its stored DN, so use the name of a user that already exists. "
            "It does not set the tasks' userName, which comes from each step's task_params."
        ),
    )
    parser.add_argument("--prod-role", action="store_true", help="Record the submitter as holding a production role, for submit_description")
    parser.add_argument("--repeat", type=int, default=1, help="How many times process should advance the workflow")
    args = parser.parse_args()
    if args.action == "submit_description":
        if not args.wfd_file:
            parser.error("submit_description needs --wfd-file")
    elif not args.workflow_id:
        parser.error(f"{args.action} needs a workflow_id")
    return args


# Data is listed in the order it flows through the workflow: what comes in, what is passed between
# steps, and what comes out. Anything unrecognised sorts last rather than being dropped.
DATA_TYPE_ORDER = (WFDataType.input, WFDataType.mid, WFDataType.output)


def data_sort_key(data_spec):
    try:
        type_rank = DATA_TYPE_ORDER.index(data_spec.type)
    except ValueError:
        type_rank = len(DATA_TYPE_ORDER)
    return type_rank, data_spec.name or ""


def step_source_label(step_spec):
    """The prodSourceLabel the step's own task parameters carry, which is not the workflow's"""
    try:
        return step_spec.definition_json_map.get("task_params", {}).get("prodSourceLabel") or "-"
    except Exception:
        return "-"


def show_workflow(task_buffer, workflow_id):
    """Print what the engine currently holds for a workflow, its steps and its data"""
    workflow_spec = task_buffer.get_workflow(workflow_id=workflow_id)
    if workflow_spec is None:
        print(f"workflow_id={workflow_id} not found")
        return
    # submitted_as is the workflow-level label, which records only whether the submitter held a
    # production role. Each step carries its own prodSourceLabel, shown as pslabel below, and that
    # is the one JEDI's agents filter on.
    print(
        f"workflow_id={workflow_spec.workflow_id} name={workflow_spec.name} status={workflow_spec.status} "
        f"submitted_as={workflow_spec.prodsourcelabel} user={workflow_spec.username}"
    )

    step_specs = task_buffer.get_steps_of_workflow(workflow_id=workflow_id) or []
    print(f"  steps ({len(step_specs)}):")
    print(f"    {'#':>3}  {'name':<24} {'status':<12} {'pslabel':<9} {'flavor':<13} target_id")
    for step_spec in sorted(step_specs, key=lambda s: s.member_id or 0):
        print(
            f"    {step_spec.member_id!s:>3}  {step_spec.name:<24} {step_spec.status:<12} "
            f"{step_source_label(step_spec):<9} {step_spec.flavor:<13} {step_spec.target_id}"
        )

    data_specs = task_buffer.get_data_of_workflow(workflow_id=workflow_id) or []
    print(f"  data ({len(data_specs)}):")
    print(f"    {'name':<28} {'status':<20} {'type':<7} target_id")
    for data_spec in sorted(data_specs, key=data_sort_key):
        # target_id still holding ${TASKID} means the producing task has not been queued yet
        print(f"    {data_spec.name:<28} {data_spec.status:<20} {data_spec.type:<7} {data_spec.target_id}")


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
