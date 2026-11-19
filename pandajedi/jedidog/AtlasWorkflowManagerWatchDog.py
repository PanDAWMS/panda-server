import datetime
import sys
import traceback
from typing import TYPE_CHECKING, Any, cast

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedibrokerage import AtlasBrokerUtils, workflow_affinity
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.taskbuffer.JediTaskSpec import WORKFLOW_HOLDUP_BROKERAGE, JediTaskSpec
from pandaserver.workflow.step_handler_plugins.panda_task_step_handler import (
    brokerage_bound_steps,
    step_task_takes_input,
)
from pandaserver.workflow.workflow_base import (
    WFDataType,
    WFStepStatus,
    WorkflowStatus,
)
from pandaserver.workflow.workflow_core import JEDI_TASK_STEP_FLAVORS, WorkflowInterface

from .WatchDogBase import WatchDogBase

if TYPE_CHECKING:
    from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
    from pandajedi.jediddm.DDMInterface import DDMInterface

logger = PandaLogger().getLogger(__name__.split(".")[-1])

# How long an anchor is left alone before the replica lookups behind it are repeated. Shorter than
# nothing would be wasteful and longer would let a workflow drift away from where its finished
# steps actually put their data.
ANCHOR_REFRESH_SEC = 900


class AtlasWorkflowManagerWatchDog(WatchDogBase):
    """
    Workflow manager watchdog for ATLAS
    """

    # constructor
    def __init__(self, taskBufferIF: "JediTaskBufferInterface", ddmIF: "DDMInterface") -> None:
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.vo = "atlas"
        self.workflow_interface = WorkflowInterface(taskBufferIF)

    def gather_anchor_inputs(self, workflow_id: int, tmpLog: MsgWrapper) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Collect what is known about where the steps of one workflow are going

        Three kinds of knowledge, which build_anchor_weights then ranks: a step a submitter pinned,
        a step that has already written its output somewhere, and a step not yet placed whose
        input sits somewhere. Only root inputs are usable for the last of these -- data produced
        inside the workflow has no dataset until its producing step runs, which is exactly why an
        unconstrained step cannot see where it ought to go on its own.

        Each source counts once per step, so a step with an output and a log dataset at one site
        is one step there, not two.

        Alongside, the state the brokerage holdup needs: which tasks are held, and which of the
        data-bound steps they wait for are not yet placed.

        Args:
            workflow_id: the workflow to look at
            tmpLog: logger of this pass

        Returns:
            tuple: (anchor inputs, holdup state). The first has lists of unified site names under
                pinned_sites, output_sites and input_sites, plus vetoed_sites, for
                build_anchor_weights. The second has held_task_specs, unplaced_bound_step_ids and
                unsettled, the last meaning that a data-free step is still to be brokered.
        """
        pinned_sites: list[str] = []
        output_sites: list[str] = []
        input_sites: list[str] = []
        vetoed_sites: set[str] = set()
        held_task_specs: list[Any] = []
        unplaced_bound_step_ids: list[int] = []
        unsettled = False

        step_specs = [step_spec for step_spec in (self.taskBufferIF.get_steps_of_workflow(workflow_id) or []) if step_spec.flavor in JEDI_TASK_STEP_FLAVORS]
        # classified over all steps, since what one step produces decides whether another is bound
        bound_step_ids = {step_spec.step_id for step_spec in brokerage_bound_steps(step_specs)}
        # the datasets this workflow takes in from outside, by the name the steps refer to them by
        dataset_of_root_input = {
            data_spec.name: data_spec.target_id
            for data_spec in (self.taskBufferIF.get_data_of_workflow(workflow_id, type_filter_list=[WFDataType.input]) or [])
            if data_spec.target_id
        }
        site_mapper = self.taskBufferIF.get_site_mapper()
        analysis_sites = [
            site_name for site_name, site_spec in site_mapper.siteSpecList.items() if site_spec.type == "analysis" or site_spec.is_grandly_unified()
        ]

        def unified(site_name: str) -> str | None:
            try:
                return site_mapper.getSite(site_name).get_unified_name()
            except Exception:
                return None

        replica_cache: dict[str, list[str]] = {}
        for step_spec in step_specs:
            is_final = step_spec.status in WFStepStatus.final_statuses
            placed = is_final
            task_spec = None
            task_id = int(step_spec.target_id) if str(step_spec.target_id or "").isdigit() else None
            if task_id:
                tmp_stat, task_spec = self.taskBufferIF.getTaskWithID_JEDI(task_id, False)
                if not tmp_stat:
                    task_spec = None
            if task_spec is not None:
                if task_spec.is_workflow_brokerage_holdup():
                    held_task_specs.append(task_spec)
                    unsettled = True
                if task_spec.status in JediTaskSpec.statusToRejectExtChange():
                    placed = True
                # a pinned task is placed before it has run anywhere
                step_pinned = {unified(one_site.strip()) for one_site in str(task_spec.site or "").split(",") if one_site.strip()}
                step_pinned.discard(None)
                if step_pinned:
                    placed = True
                    pinned_sites.extend(cast(set[str], step_pinned))
                tmp_stat, dataset_specs = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(task_id, ["output", "log"])
                # an analysis output is instantiated per site, and the site it was instantiated
                # for is where the data actually is
                step_outputs = {unified(dataset_spec.site) for dataset_spec in dataset_specs or [] if dataset_spec.site}
                step_outputs.discard(None)
                if step_outputs:
                    placed = True
                    output_sites.extend(cast(set[str], step_outputs))
            elif not is_final and step_task_takes_input(step_spec) is False:
                # a data-free step not yet submitted: it will be held on submission
                unsettled = True
            if step_spec.step_id in bound_step_ids and not placed:
                unplaced_bound_step_ids.append(step_spec.step_id)
            if placed:
                continue
            # Not placed yet, submitted or not. Where its root inputs are is the only hint, and it
            # is the one that lets a step with no input of its own learn where its siblings must run.
            for input_name in step_spec.definition_json_map.get("input_data_dict", {}):
                dataset_name = dataset_of_root_input.get(input_name)
                if not dataset_name:
                    continue
                if dataset_name not in replica_cache:
                    replica_cache[dataset_name] = self.sites_holding_dataset(analysis_sites, site_mapper, dataset_name, tmpLog)
                input_sites.extend(replica_cache[dataset_name])

        anchor_inputs = {
            "pinned_sites": pinned_sites,
            "output_sites": output_sites,
            "input_sites": input_sites,
            "vetoed_sites": vetoed_sites,
        }
        holdup_state = {
            "held_task_specs": held_task_specs,
            "unplaced_bound_step_ids": unplaced_bound_step_ids,
            "unsettled": unsettled,
        }
        return anchor_inputs, holdup_state

    def sites_holding_dataset(
        self,
        analysis_sites: list[str],
        site_mapper: Any,
        dataset_name: str,
        tmpLog: MsgWrapper,
    ) -> list[str]:
        """
        The analysis sites holding a dataset on disk, by unified name

        Asked the same way brokerage will ask it later, so that a site preferred here is one
        brokerage would also call local rather than one it would reject.
        """
        try:
            # self.ddmIF is the multi-VO wrapper; the replica methods live on the per-VO client,
            # which is what brokerage is handed too
            ddm_if = self.ddmIF.getInterface(self.vo)
            if ddm_if is None:
                tmpLog.debug(f"no DDM interface for vo={self.vo}")
                return []
            tmp_stat, data_site_map = AtlasBrokerUtils.get_sites_with_data(analysis_sites, site_mapper, ddm_if, dataset_name, None, 10, 100)[:2]
            # on any failure the second element is a message rather than the map
            if tmp_stat != Interaction.SC_SUCCEEDED or not isinstance(data_site_map, dict):
                tmpLog.debug(f"no replica information for {dataset_name}: {data_site_map}")
                return []
            return AtlasBrokerUtils.getAnalSitesWithDataDisk(data_site_map, includeTape=False)
        except Exception as e:
            # A preference that cannot be computed is not a reason to disturb the workflow.
            tmpLog.debug(f"failed to locate {dataset_name}: {e}")
            return []

    def anchor_is_fresh(self, workflow_spec: Any) -> bool:
        """
        Whether this workflow's anchor was computed recently enough to leave alone

        The placement of a workflow changes slowly -- a step has to finish, or a new one has to be
        submitted -- while the replica lookups behind an anchor are the expensive part of this
        pass. A workflow with no anchor at all is never fresh, so a newly registered one is always
        computed on the cycle it appears. Nor is one whose data-free steps are still to be
        brokered: those are waiting on this pass to be released, and it must look every cycle.
        """
        anchor: dict[str, Any] | None = workflow_spec.get_parameter(workflow_affinity.WORKFLOW_ANCHOR_PARAM)
        if not anchor or not anchor.get("computed_at") or anchor.get("unsettled"):
            return False
        try:
            age = naive_utcnow() - datetime.datetime.fromisoformat(str(anchor["computed_at"]))
        except Exception:
            return False
        return bool(age.total_seconds() < ANCHOR_REFRESH_SEC)

    def release_brokerage_holdup(self, task_spec: Any, tmpLog: MsgWrapper) -> bool:
        """
        Let a task held for its workflow's brokerage go on to be brokered

        The same steps on_all_inputs_done takes for the input hold, clearing only the brokerage
        bit. If the contents feeder holds the task's lock just now, it may write the old rule
        back; the task then still shows as held, and the next pass releases it again.

        Returns:
            bool: whether the task was released
        """
        task_id = task_spec.jediTaskID
        task_spec.set_workflow_holdup(False, WORKFLOW_HOLDUP_BROKERAGE)
        self.taskBufferIF.updateTask_JEDI(task_spec, {"jediTaskID": task_id}, updateDEFT=False)
        if task_spec.status == "pending":
            if not self.taskBufferIF.release_task_on_hold(task_id):
                tmpLog.warning(f"task_id={task_id} brokerage holdup unset but not released from pending; retried next cycle")
                return False
        self.taskBufferIF.push_task_trigger_message("jedi_contents_feeder", task_id)
        tmpLog.info(f"task_id={task_id} released from brokerage holdup")
        return True

    def doComputeWorkflowAnchors(self) -> None:
        """
        Record, on each active workflow, which sites the rest of it is going to, and release the
        data-free steps that were waiting for it

        Runs before the workflows are processed, because an analysis task is job-generated almost
        as soon as it is submitted: a small step with no input can be placed in its first pass, and
        that is exactly the step this preference exists to move. A workflow that first appears
        during this cycle therefore gets its anchor before any of its steps submits a task.

        A data-free step is held on submission until its data-bound siblings are placed. Once they
        are, the anchor stored here already reflects where they went, and the held steps are
        released together so that they are brokered back to back against it.
        """
        tmpLog = MsgWrapper(logger, " #ATM #KV doComputeWorkflowAnchors")
        tmpLog.debug("start")
        try:
            if not self.taskBufferIF.getConfigValue("anal_jobbroker", "WORKFLOW_AFFINITY", "jedi", self.vo):
                tmpLog.debug("workflow affinity is off; skipped")
                return
            got_lock = self.get_process_lock("AtlasWFManagerDog.doComputeWorkflowAnchors", timeLimit=5)
            if not got_lock:
                tmpLog.debug("locked by another watchdog process. Skipped")
                return
            # check_interval_sec=0 so that nothing active is held back: a workflow registered this
            # cycle must be anchored now, before its first step submits a task. Workflows that
            # already carry a fresh anchor are skipped below instead, which costs one comparison
            # rather than the replica lookups.
            workflow_specs = (
                self.taskBufferIF.query_workflows(
                    status_filter_list=list(WorkflowStatus.active_statuses),
                    check_interval_sec=0,
                )
                or []
            )
            n_anchored = 0
            n_fresh = 0
            n_released = 0
            for queried_spec in workflow_specs:
                workflow_id = queried_spec.workflow_id
                try:
                    if self.anchor_is_fresh(queried_spec):
                        n_fresh += 1
                        continue
                    # the gathering touches the database and DDM, so it is done outside the lock
                    anchor_inputs, holdup_state = self.gather_anchor_inputs(workflow_id, tmpLog)
                    weights = workflow_affinity.build_anchor_weights(**anchor_inputs)
                    if weights:
                        # The lock re-reads the workflow, which matters because parameters is a
                        # single JSON column the engine also writes: storing against a spec read
                        # earlier would drop whatever it recorded meanwhile.
                        with self.workflow_interface.workflow_lock(workflow_id) as workflow_spec:
                            if workflow_spec is None:
                                # nothing is released either: held tasks must see the new anchor
                                tmpLog.debug(f"workflow_id={workflow_id} locked by the engine; skipped")
                                continue
                            if workflow_spec.status not in WorkflowStatus.active_statuses:
                                continue
                            workflow_spec.set_parameter(
                                workflow_affinity.WORKFLOW_ANCHOR_PARAM,
                                {
                                    "weights": weights,
                                    "computed_at": naive_utcnow().isoformat(),
                                    "unsettled": holdup_state["unsettled"],
                                },
                            )
                            self.taskBufferIF.update_workflow(workflow_spec)
                        n_anchored += 1
                        tmpLog.debug(f"workflow_id={workflow_id} anchor: {workflow_affinity.summarize_anchor({'weights': weights})}")
                    # Release only after the anchor is stored, so the released tasks read the one
                    # computed from where their siblings actually went.
                    held_task_specs = holdup_state["held_task_specs"]
                    if held_task_specs:
                        unplaced = holdup_state["unplaced_bound_step_ids"]
                        if unplaced:
                            tmpLog.debug(f"workflow_id={workflow_id} {len(held_task_specs)} held task(s) wait for data-bound steps {sorted(unplaced)}")
                        else:
                            for task_spec in held_task_specs:
                                if self.release_brokerage_holdup(task_spec, tmpLog):
                                    n_released += 1
                except Exception as e:
                    tmpLog.warning(f"failed to anchor workflow_id={workflow_id} with {e}")
            tmpLog.info(f"anchored {n_anchored} of {len(workflow_specs)} active workflows, {n_fresh} already fresh, released {n_released} held tasks")
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    def doProcessWorkflows(self) -> None:
        """
        Action to process active workflows
        """
        tmpLog = MsgWrapper(logger, " #ATM #KV doProcessWorkflows")
        tmpLog.debug("start")
        try:
            # watchdog lock
            got_lock = self.get_process_lock("AtlasWFManagerDog.doProcessWorkflows", timeLimit=5)
            if not got_lock:
                tmpLog.debug("locked by another watchdog process. Skipped")
                return
            tmpLog.debug("got watchdog lock")
            # process active workflows
            stats = self.workflow_interface.process_active_workflows()
            tmpLog.info(f"processed workflows: {stats}")
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    # main
    def doAction(self) -> Interaction.StatusCode:
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug("start")
            # clean up
            # check
            # record where each workflow is going, before anything submits a task
            self.doComputeWorkflowAnchors()
            # process workflows
            self.doProcessWorkflows()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            origTmpLog.error(f"failed with {errtype} {errvalue}")
        # return
        origTmpLog.debug("done")
        return self.SC_SUCCEEDED
