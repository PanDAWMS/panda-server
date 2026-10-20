import argparse
import json
import sys
from typing import TYPE_CHECKING

from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer, taskBuffer
from pandaserver.workflow.workflow_base import WFStepSpec, data_sort_key
from pandaserver.workflow.workflow_base import step_source_label as workflow_base_step_source_label

if TYPE_CHECKING:
    # imported inside main() at runtime, since importing it pulls in the DDM client
    from pandaserver.workflow.workflow_core import WorkflowInterface


def parse_args() -> argparse.Namespace:
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
            "  everything a client shows in one answer, steps with progress and data together:\n"
            "    %(prog)s details 12345\n"
            "  see which step feeds which, as a consumer outside the engine would ask:\n"
            "    %(prog)s relations 12345\n"
            "  the same as JEDI tasks only, which is the view DEFT wants:\n"
            "    %(prog)s task_relations 12345\n"
            "    %(prog)s task_relations --task-id 52401216\n"
            "  cancel it:\n"
            "    %(prog)s cancel_workflow 12345 --force\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "action",
        choices=["submit_description", "show", "process", "details", "relations", "task_relations", "cancel_workflow"],
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
    parser.add_argument("--task-id", type=int, help="Enter by JEDI task ID instead of workflow ID, for task_relations")
    args = parser.parse_args()
    if args.action == "submit_description":
        if not args.wfd_file:
            parser.error("submit_description needs --wfd-file")
    elif args.action == "task_relations" and args.task_id:
        pass
    elif not args.workflow_id:
        parser.error(f"{args.action} needs a workflow_id")
    return args


def step_source_label(step_spec: WFStepSpec) -> str:
    """The step's own prodSourceLabel, shown as '-' when it has none"""
    return workflow_base_step_source_label(step_spec) or "-"


def show_workflow(task_buffer: TaskBuffer, workflow_id: int) -> None:
    """Print what the engine currently holds for a workflow, its steps and its data"""
    workflow_spec = task_buffer.get_workflow(workflow_id=workflow_id)
    if workflow_spec is None:
        print(f"workflow_id={workflow_id} not found")
        return
    # submitted_as is the workflow-level label, which records only whether the submitter held a
    # production role. A panda_task step carries its own prodSourceLabel, shown as pslabel after
    # the flavor it belongs to, and that is the one JEDI's agents filter on; a flavor that maps to
    # something other than a PanDA task has no such label and shows '-'.
    print(
        f"workflow_id={workflow_spec.workflow_id} name={workflow_spec.name} status={workflow_spec.status} "
        f"submitted_as={workflow_spec.prodsourcelabel} user={workflow_spec.username}"
    )

    step_specs = task_buffer.get_steps_of_workflow(workflow_id=workflow_id) or []
    print(f"  steps ({len(step_specs)}):")
    print(f"    {'#':>3}  {'name':<24} {'status':<12} {'flavor':<13} {'pslabel':<9} target_id")
    for step_spec in sorted(step_specs, key=lambda s: s.member_id or 0):
        print(
            f"    {step_spec.member_id!s:>3}  {step_spec.name:<24} {step_spec.status:<12} "
            f"{step_spec.flavor:<13} {step_source_label(step_spec):<9} {step_spec.target_id}"
        )

    data_specs = task_buffer.get_data_of_workflow(workflow_id=workflow_id) or []
    print(f"  data ({len(data_specs)}):")
    print(f"    {'name':<28} {'status':<20} {'type':<7} target_id")
    for data_spec in sorted(data_specs, key=data_sort_key):
        # target_id still holding ${TASKID} means the producing task has not been queued yet
        print(f"    {data_spec.name:<28} {data_spec.status:<20} {data_spec.type:<7} {data_spec.target_id}")


def show_workflow_details(wfif: "WorkflowInterface", workflow_id: int) -> None:
    """Print the one answer a client gets, so that what pbook shows can be checked against the DB

    This is the only way to exercise the file-progress query, which reads JEDI_Datasets and so has
    no offline equivalent.
    """
    details = wfif.get_workflow_details(workflow_id)
    if details is None:
        print(f"no details for workflow_id={workflow_id}")
        return
    print(
        f"workflow_id={details['workflow_id']} name={details['name']} status={details['status']} submitted_as={details['submitted_as']} user={details['user']}"
    )
    if details.get("parent_id"):
        print(f"  sub-workflow of workflow_id={details['parent_id']}")
    if details.get("reason"):
        print(f"  reason: {details['reason']}")
    print(f"  step status counts: {details['step_status_counts']}")

    name_of_step_id = {step["step_id"]: step["name"] for step in details["steps"]}
    print(f"  steps ({len(details['steps'])}):")
    print(f"    {'#':>3}  {'name':<24} {'status':<12} {'flavor':<13} {'pslabel':<9} {'progress':>8}  {'files d|f|t':<14} {'target_id':<12} parents")
    for step in details["steps"]:
        progress = step["progress"]
        # a step whose target is not a task, or which has not been submitted, has nothing to measure
        pct = f"{progress['pct_finished']}%" if progress else "-"
        files = f"{progress['n_files_finished']}|{progress['n_files_failed']}|{progress['n_files']}" if progress else "-"
        parents = ", ".join(name_of_step_id.get(parent_id, str(parent_id)) for parent_id in step["parent_step_ids"]) or "-"
        print(
            f"    {step['member_id']!s:>3}  {step['name']:<24} {step['status']:<12} {step['flavor']:<13} "
            f"{str(step['prod_source_label'] or '-'):<9} {pct:>8}  {files:<14} {str(step['target_id'] or '-'):<12} {parents}"
        )

    print(f"  data ({len(details['data'])}):")
    print(f"    {'name':<28} {'status':<20} {'type':<7} target_id")
    for data in details["data"]:
        print(f"    {data['name']:<28} {data['status']:<20} {data['type']:<7} {data['target_id']}")


def show_step_relations(wfif: "WorkflowInterface", workflow_id: int) -> None:
    """Print which step feeds which, derived from the data passed between them"""
    relations = wfif.get_step_relations(workflow_id)
    if relations is None:
        print(f"no step relations for workflow_id={workflow_id}")
        return
    steps = relations["steps"]
    name_of_step = {step["step_id"]: step["name"] for step in steps}
    print(f"workflow_id={relations['workflow_id']} step relations ({len(steps)}):")
    print(f"    {'step':>4}  {'name':<24} {'status':<12} {'flavor':<13} {'target_id':<12} parents")
    for step in steps:
        # A step with no parent takes only data produced outside the workflow. A step that has not
        # started has no target_id yet, while its place in the graph is already known.
        parents = ", ".join(f"{name_of_step[parent]}({parent})" for parent in step["parent_step_ids"]) or "-"
        print(f"    {step['step_id']!s:>4}  {step['name']:<24} {step['status']:<12} {step['flavor']:<13} {step['target_id'] or '-':<12} {parents}")


def show_task_relations(wfif: "WorkflowInterface", workflow_id: int | None, task_id: int | None) -> None:
    """Print the JEDI-task view: non-task steps collapsed, nested workflows descended into"""
    if task_id is not None:
        relations = wfif.get_task_relations_of_task(task_id)
    elif workflow_id is not None:
        relations = wfif.get_task_relations(workflow_id)
    else:
        # parse_args requires one of the two, so this is a programming error rather than misuse
        raise ValueError("task_relations needs a workflow_id or --task-id")
    if relations is None:
        print(f"no task relations for {'task_id=' + str(task_id) if task_id is not None else 'workflow_id=' + str(workflow_id)}")
        return
    tasks = relations["tasks"]
    name_of_key = {task["key"]: task["name"] for task in tasks}
    asked_for = relations.get("asked_for")
    print(f"workflow_id={relations['workflow_id']} task relations ({len(tasks)}):")
    print(f"    {'task_id':<12} {'workflow':>8}  {'name':<24} {'status':<12} parents")
    for task in tasks:
        # No task_id means the step has not been submitted yet; the row is a placeholder so that
        # a task whose producer has not started is not mistaken for one with no producer.
        parents = ", ".join(name_of_key.get(key, key) for key in task["parents"]) or "-"
        marker = " <-- asked for" if task["key"] == asked_for else ""
        print(f"    {str(task['task_id'] or '-'):<12} {task['workflow_id']!s:>8}  {task['name']:<24} {task['status']:<12} {parents}{marker}")


def main() -> None:
    args = parse_args()
    WFID = args.workflow_id

    from pandaserver.workflow.workflow_core import WorkflowInterface

    # interface for workflow operations
    requester_id = GenericThread().get_full_id(__name__, __file__)
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
    elif args.action == "details":
        show_workflow_details(wfif, WFID)
    elif args.action == "relations":
        show_step_relations(wfif, WFID)
    elif args.action == "task_relations":
        show_task_relations(wfif, WFID, args.task_id)
    elif args.action == "process":
        # Advance the workflow by hand instead of waiting for the WatchDog cycle
        for attempt in range(args.repeat):
            workflow_spec = taskBuffer.get_workflow(workflow_id=WFID)
            if workflow_spec is None:
                print(f"workflow_id={WFID} not found")
                return
            before = workflow_spec.status
            process_result, workflow_spec = wfif.process_workflow(workflow_spec)
            print(f"  [{attempt + 1}] {before} -> {workflow_spec.status} success={process_result.success} {process_result.message}")
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
