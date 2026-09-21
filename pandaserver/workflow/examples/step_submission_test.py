"""
Offline check of workflow step task submission.

Covers the pieces that turn a parsed step into a queued task: resolving input dataset references
against the datasets actually produced upstream, refusing a production label without the role,
resolving the late-bound task ID into the step's output dataset names, and the task status mapping.

Run from the repository root:  python3 pandaserver/workflow/examples/step_submission_test.py
"""

import copy
import importlib.abc
import importlib.machinery
import json
import os
import sys
import types

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

AUTO_STUB_ROOTS = ("idds", "pandaclient", "ruamel", "requests", "snakemake")


class AutoStubFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in AUTO_STUB_ROOTS:
            return importlib.machinery.ModuleSpec(name, self, is_package=True)
        return None

    def create_module(self, spec):
        module = types.ModuleType(spec.name)
        module.__path__ = []
        return module

    def exec_module(self, module):
        class Anything:
            def __init__(self, *a, **k):
                pass

            def __call__(self, *a, **k):
                return self

        module.__getattr__ = lambda name: Anything


sys.meta_path.insert(0, AutoStubFinder())


def stub(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


LOGGED: dict[str, list[str]] = {"warning": [], "error": []}


class Log:
    def __init__(self, *a, **k):
        pass

    def info(self, m):
        pass

    def debug(self, m):
        pass

    def warning(self, m):
        LOGGED["warning"].append(m)

    def error(self, m):
        LOGGED["error"].append(m)


stub("pandacommon")
pandautils = stub("pandacommon.pandautils")
pandautils.__path__ = []
stub("pandacommon.pandautils.base", SpecBase=object)
pandalogger = stub("pandacommon.pandalogger")
pandalogger.__path__ = []
stub("pandacommon.pandalogger.LogWrapper", LogWrapper=Log)
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda: types.SimpleNamespace(getLogger=lambda n: None))
stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))

from pandaserver.workflow.step_handler_plugins.panda_task_step_handler import (  # noqa: E402
    PandaTaskStepHandler,
)
from pandaserver.workflow.workflow_base import (  # noqa: E402
    PARENT_TASKID_PLACEHOLDER,
    TASKID_PLACEHOLDER,
    WFStepSpec,
    WFStepStatus,
)


class FakeData:
    def __init__(self, name, target_id, source_step_id=None):
        self.name = name
        self.target_id = target_id
        self.workflow_id = 1
        self.data_id = abs(hash(name)) % 1000
        # which step produced it, which is what ${PARENT_TASKID} resolves through
        self.source_step_id = source_step_id


class FakeStep(WFStepSpec):
    def __init__(self, definition, parameters=None):
        self.workflow_id = 1
        self.step_id = 7
        self.name = "probe"
        self.flavor = "panda_task"
        self.target_id = None
        self.status = WFStepStatus.ready
        # definition_json and parameters are the real backing attributes, so the spec's own
        # definition_json_map and get/set_parameter do the work and are exercised as written
        self.definition_json = json.dumps(definition)
        self.parameters = json.dumps(parameters or {})


class FakeTaskBuffer:
    def __init__(self, data_by_name=None, task_id=49900001, error="", deft_status=None, steps_by_id=None):
        self.deft_status = deft_status
        self.data = data_by_name or {}
        self.steps = steps_by_id or {}
        self.task_id = task_id
        self.error = error
        self.inserted = []
        self.inserted_parent_tids = []
        self.updated_data = []

    def get_workflow_step(self, step_id):
        return self.steps.get(step_id)

    def get_steps_of_workflow(self, workflow_id, status_filter_list=None, status_exclusion_list=None):
        return list(self.steps.values())

    def get_workflow_data_by_name(self, name, workflow_id):
        return self.data.get(name)

    def update_workflow_data(self, data_spec):
        self.updated_data.append((data_spec.name, data_spec.target_id))

    def update_workflow_step(self, step_spec):
        pass

    def insert_step_task(self, task_params_map, user_dn, parent_tid=None):
        self.inserted.append(copy.deepcopy(task_params_map))
        self.inserted_parent_tids.append(parent_tid)
        if self.task_id is None:
            return None, self.error
        return self.task_id, ""

    def getTaskStatusSuperstatus(self, task_id):
        return self._status

    def get_deft_task_status(self, task_id):
        return self.deft_status

    def set_status(self, status, superstatus=None):
        # a real getTaskStatusSuperstatus returns a falsy value when the task is not in JEDI
        self._status = None if status is None else (status, superstatus or status)

    def getTaskWithID_JEDI(self, task_id, *args, **kwargs):
        # mirrors the real signature: (found, task_spec); None when the task is not in JEDI yet
        return (False, None) if self._status is None else (True, None)


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def main():
    failures = 0
    wfd = json.load(open(os.path.join(os.path.dirname(__file__), "production_chain_wfd.json")))
    # the simul step consumes {merge_evnt/EVNT} and produces one HITS dataset
    simul_params = copy.deepcopy(wfd["steps"]["simul"]["task_params"])
    # resolve ${WFID} the way registration would, so this works whether or not the description uses it
    simul_params = json.loads(json.dumps(simul_params).replace("${WFID}", "12345"))

    def make_step(prod_role=True, all_inputs_complete=True, params=None):
        return FakeStep(
            {
                "task_params": copy.deepcopy(params if params is not None else simul_params),
                "user_dn": "/DC=ch/CN=test",
                "prod_role": prod_role,
                "output_data_list": ["simul/HITS"],
            },
            {"all_inputs_complete": all_inputs_complete},
        )

    produced_evnt = "mc23_13p6TeV.526140.x.merge.EVNT.e8590_e8586_wfid12345_tid48810699_00"

    def make_tbif(**kw):
        data = {
            "merge_evnt/EVNT": FakeData("merge_evnt/EVNT", produced_evnt),
            "simul/HITS": FakeData("simul/HITS", f"mc23_13p6TeV.526140.x.simul.HITS.e8590_e8586_a934_wfid12345_tid{TASKID_PLACEHOLDER}_00"),
        }
        return FakeTaskBuffer(data_by_name=data, **kw)

    print("\n=== submit_target: the happy path ===")
    tbif = make_tbif()
    handler = PandaTaskStepHandler(tbif)
    step = make_step()
    res = handler.submit_target(step)
    failures += not check("submitted", res.success is True, res.message)
    failures += not check("target_id is the task id", res.target_id == "49900001", res.target_id)
    submitted = tbif.inserted[0]
    inputs = [p["dataset"] for p in submitted["jobParameters"] if p.get("param_type") == "input"]
    failures += not check("input reference resolved to the produced dataset", inputs == [produced_evnt], inputs)
    failures += not check("no brace reference left in any input dataset", not any("{" in d for d in inputs), inputs)
    # Output dataset names still carry ${TASKID} at this point on purpose: the ID does not exist
    # until the insert allocates it, so insert_step_task resolves it inside the same transaction.
    outputs = [p["dataset"] for p in submitted["jobParameters"] if p.get("param_type") == "output"]
    failures += not check("output datasets still carry the placeholder for the DB layer to resolve", all(TASKID_PLACEHOLDER in d for d in outputs), outputs)
    failures += not check("pseudo_input left untouched", any(p.get("dataset") == "seq_number" for p in submitted["jobParameters"]))
    failures += not check("workflowHoldup not set when inputs are complete", "workflowHoldup" not in submitted)
    failures += not check(
        "output dataset name resolved from the task id",
        tbif.updated_data == [("simul/HITS", "mc23_13p6TeV.526140.x.simul.HITS.e8590_e8586_a934_wfid12345_tid49900001_00")],
        tbif.updated_data,
    )
    failures += not check("submission attempt recorded", step.get_parameter("submit_attempt_task_name") == simul_params["taskName"])

    print("\n=== workflowHoldup is set while inputs are incomplete ===")
    tbif = make_tbif()
    handler = PandaTaskStepHandler(tbif)
    handler.submit_target(make_step(all_inputs_complete=False))
    failures += not check("workflowHoldup set", tbif.inserted[0].get("workflowHoldup") is True)

    print("\n=== a production label without the role is refused ===")
    # only the task-level production label is guarded; see PRODUCTION_SOURCE_LABELS
    for label in ["managed"]:
        params = copy.deepcopy(simul_params)
        params["prodSourceLabel"] = label
        tbif = make_tbif()
        handler = PandaTaskStepHandler(tbif)
        res = handler.submit_target(make_step(prod_role=False, params=params))
        failures += not check(f"{label} refused without the role", res.success is not True)
        failures += not check(f"{label} reason mentions the production role", "production role" in res.message, res.message)
        failures += not check(f"{label} submitted nothing", tbif.inserted == [])
        # ... and is accepted once the submitter holds it
        tbif = make_tbif()
        handler = PandaTaskStepHandler(tbif)
        res = handler.submit_target(make_step(prod_role=True, params=params))
        failures += not check(f"{label} accepted with the role", res.success is True, res.message)
    # non-production task labels need no role. "test" is included deliberately: a JEDI instance may
    # route it to the production refiner, but which task labels it accepts is its own configuration,
    # so the server does not gate it. "prod_test" is a job-level label and never reaches here.
    for label in ["user", "test", "ptest"]:
        params = copy.deepcopy(simul_params)
        params["prodSourceLabel"] = label
        tbif = make_tbif()
        handler = PandaTaskStepHandler(tbif)
        res = handler.submit_target(make_step(prod_role=False, params=params))
        failures += not check(f"{label} needs no production role", res.success is True, res.message)

    print("\n=== an unresolved upstream output blocks submission ===")
    tbif = make_tbif()
    tbif.data["merge_evnt/EVNT"].target_id = f"mc23...merge.EVNT.e8590_wfid12345_tid{TASKID_PLACEHOLDER}_00"
    handler = PandaTaskStepHandler(tbif)
    res = handler.submit_target(make_step())
    failures += not check("refused", res.success is not True)
    failures += not check("reason mentions it is not resolved yet", "not resolved yet" in res.message, res.message)
    failures += not check("nothing submitted", tbif.inserted == [])

    print("\n=== a missing upstream output blocks submission ===")
    tbif = make_tbif()
    del tbif.data["merge_evnt/EVNT"]
    handler = PandaTaskStepHandler(tbif)
    res = handler.submit_target(make_step())
    failures += not check("refused", res.success is not True)
    failures += not check("nothing submitted", tbif.inserted == [])

    print("\n=== a second attempt for the same taskName is refused ===")
    tbif = make_tbif()
    handler = PandaTaskStepHandler(tbif)
    step = make_step()
    handler.submit_target(step)
    res2 = handler.submit_target(step)
    failures += not check("second attempt refused", res2.success is not True)
    failures += not check("reason mentions the previous attempt", "previous attempt" in res2.message, res2.message)
    failures += not check("submitted only once", len(tbif.inserted) == 1, len(tbif.inserted))

    print("\n=== a literal (external) dataset is passed through ===")
    recon_params = json.loads(json.dumps(copy.deepcopy(wfd["steps"]["recon"]["task_params"])).replace("${WFID}", "12345"))
    tbif = make_tbif()
    tbif.data["merge_hits/HITS"] = FakeData("merge_hits/HITS", "mc23...merge.HITS..._tid48810713_00")
    tbif.data["rdo_bkg"] = FakeData("rdo_bkg", wfd["inputs"]["rdo_bkg"])
    handler = PandaTaskStepHandler(tbif)
    step = FakeStep({"task_params": recon_params, "user_dn": "/DC=ch/CN=t", "prod_role": True, "output_data_list": []}, {"all_inputs_complete": True})
    res = handler.submit_target(step)
    failures += not check("submitted", res.success is True, res.message)
    datasets = [p["dataset"] for p in tbif.inserted[0]["jobParameters"] if p.get("param_type") == "input"]
    failures += not check("both inputs resolved", set(datasets) == {"mc23...merge.HITS..._tid48810713_00", wfd["inputs"]["rdo_bkg"]}, datasets)

    print("\n=== a task submitted but not yet refined into JEDI ===")
    # JEDI has no row yet, but DEFT does: expected for up to a refiner cycle, so this must be a
    # warning naming the situation, not an error suggesting the task was lost.
    del LOGGED["warning"][:], LOGGED["error"][:]
    tbif = make_tbif(deft_status="waiting")
    tbif.set_status(None)
    handler = PandaTaskStepHandler(tbif)
    step = make_step()
    step.status = WFStepStatus.running
    step.target_id = "49900001"
    result = handler.check_target(step)
    failures += not check("check_target does not advance the step", not result.success)
    # the tri-state contract: None means "not checkable yet", which the caller treats as a wait
    failures += not check("success is None, not False", result.success is None, result.success)
    failures += not check("logged as a warning, not an error", LOGGED["warning"] and not LOGGED["error"], (LOGGED["warning"], LOGGED["error"]))
    failures += not check("message explains it is queued in DEFT", "queued in DEFT" in result.message and "not yet refined" in result.message, result.message)

    del LOGGED["warning"][:], LOGGED["error"][:]
    handler.on_all_inputs_done(step)
    failures += not check("on_all_inputs_done also warns rather than errors", LOGGED["warning"] and not LOGGED["error"], (LOGGED["warning"], LOGGED["error"]))

    print("\n=== a task missing from both JEDI and DEFT is still an error ===")
    del LOGGED["warning"][:], LOGGED["error"][:]
    tbif = make_tbif(deft_status=None)
    tbif.set_status(None)
    handler = PandaTaskStepHandler(tbif)
    step = make_step()
    step.status = WFStepStatus.running
    step.target_id = "49900001"
    result = handler.check_target(step)
    failures += not check("logged as an error", LOGGED["error"] and not LOGGED["warning"], (LOGGED["warning"], LOGGED["error"]))
    failures += not check("message says neither JEDI nor DEFT", "not found in JEDI or DEFT" in result.message, result.message)
    failures += not check("success is False, so the caller reports a failure", result.success is False, result.success)

    print("\n=== the tri-state is honoured by every check_target return path ===")
    # None for the cases that are waits or skips, False only for genuine failures
    for label, setup, expected in [
        ("step not in a checkable status", lambda st, tb: setattr(st, "status", WFStepStatus.pending), None),
        # a flavor mismatch is a routing bug, not a wait: False so the caller reports it
        ("wrong flavor for this handler", lambda st, tb: setattr(st, "flavor", "something_else"), False),
        ("target not submitted yet", lambda st, tb: setattr(st, "target_id", None), None),
        ("unrecognised native status", lambda st, tb: tb.set_status("nonsense_status"), False),
    ]:
        tbif = make_tbif(deft_status="waiting")
        tbif.set_status("running")
        step = make_step()
        step.status = WFStepStatus.running
        step.target_id = "49900001"
        setup(step, tbif)
        got = PandaTaskStepHandler(tbif).check_target(step).success
        failures += not check(f"{label} -> success {expected}", got is expected, f"got {got}")

    print("\n=== a flavor mismatch fails loudly on every entry point ===")
    for name in ("submit_target", "check_target", "cancel_target", "on_all_inputs_done"):
        del LOGGED["warning"][:], LOGGED["error"][:]
        tbif = make_tbif()
        tbif.set_status("running")
        step = make_step()
        step.status = WFStepStatus.running
        step.target_id = "49900001"
        step.flavor = "something_else"
        result = getattr(PandaTaskStepHandler(tbif), name)(step)
        failures += not check(f"{name} logs an error, not a warning", LOGGED["error"] and not LOGGED["warning"], (LOGGED["warning"], LOGGED["error"]))
        failures += not check(f"{name} names the wrong handler", any("wrong step handler" in m for m in LOGGED["error"]), LOGGED["error"])
        if result is not None:
            failures += not check(f"{name} reports success False", result.success is False, result.success)
        failures += not check(f"{name} did nothing", tbif.inserted == [] and tbif.updated_data == [])

    print("\n=== ${PARENT_TASKID}: resolved from the step that produced the input ===")

    def make_parent_step(parent_tid_value, noWaitParent=True, inputs=("merge_evnt/EVNT",)):
        params = copy.deepcopy(simul_params)
        params["parent_tid"] = parent_tid_value
        if noWaitParent:
            params["noWaitParent"] = True
        else:
            params.pop("noWaitParent", None)
        step = make_step(params=params)
        definition = step.definition_json_map
        definition["input_data_dict"] = {name: {} for name in inputs}
        step.definition_json = json.dumps(definition)
        return step

    # merge_evnt/EVNT was produced by step 3, whose task is 48810699
    def parent_tbif(**kw):
        data = {
            "merge_evnt/EVNT": FakeData("merge_evnt/EVNT", produced_evnt, source_step_id=3),
            "simul/HITS": FakeData("simul/HITS", f"...tid{TASKID_PLACEHOLDER}_00"),
        }
        data.update(kw.pop("extra_data", {}))
        feeding = FakeStep({}, None)
        feeding.step_id, feeding.name = 3, "merge_evnt"
        return FakeTaskBuffer(data_by_name=data, steps_by_id={3: feeding}, **kw)

    tbif = parent_tbif()
    tbif.steps[3].target_id = "48810699"
    res = PandaTaskStepHandler(tbif).submit_target(make_parent_step(PARENT_TASKID_PLACEHOLDER))
    failures += not check("the task is submitted", res.success is True, res.message)
    failures += not check("with the producing step's task as parent", tbif.inserted_parent_tids == [48810699], tbif.inserted_parent_tids)
    failures += not check("and parent_tid is taken out of the task params", "parent_tid" not in tbif.inserted[0], sorted(tbif.inserted[0])[:5])

    print("\n=== a step with no parent inside the workflow ===")
    tbif = parent_tbif(extra_data={"rdo_bkg": FakeData("rdo_bkg", "external.dataset", source_step_id=None)})
    res = PandaTaskStepHandler(tbif).submit_target(make_parent_step(PARENT_TASKID_PLACEHOLDER, inputs=("rdo_bkg",)))
    failures += not check("is submitted", res.success is True, res.message)
    failures += not check("with no parent, so JEDI makes the task its own", tbif.inserted_parent_tids == [None], tbif.inserted_parent_tids)

    print("\n=== a step that does not ask for it is unaffected ===")
    tbif = parent_tbif()
    res = PandaTaskStepHandler(tbif).submit_target(make_step())
    failures += not check("submitted with no parent", res.success is True and tbif.inserted_parent_tids == [None], tbif.inserted_parent_tids)

    print("\n=== a literal task ID is passed through ===")
    tbif = parent_tbif()
    tbif.steps[3].target_id = "48810699"
    PandaTaskStepHandler(tbif).submit_target(make_parent_step(52397622))
    failures += not check("as given, without consulting the graph", tbif.inserted_parent_tids == [52397622], tbif.inserted_parent_tids)

    print("\n=== a joining step is refused rather than guessed at ===")
    tbif = parent_tbif(
        extra_data={
            "left/OUT": FakeData("left/OUT", "a", source_step_id=3),
            "right/OUT": FakeData("right/OUT", "b", source_step_id=4),
        }
    )
    tbif.steps[3].target_id = "52000003"
    tbif.steps[4] = FakeStep({}, None)
    tbif.steps[4].step_id, tbif.steps[4].name = 4, "right"
    tbif.steps[4].target_id = "52000004"
    res = PandaTaskStepHandler(tbif).submit_target(make_parent_step(PARENT_TASKID_PLACEHOLDER, inputs=("left/OUT", "right/OUT")))
    failures += not check("not submitted", res.success is not True, res.success)
    failures += not check("and says it is ambiguous", "ambiguous" in res.message, res.message)
    failures += not check("and offers the named form", "PARENT_TASKID:<step name>" in res.message, res.message)
    failures += not check("nothing was inserted", tbif.inserted == [], tbif.inserted)

    print("\n=== a value that is neither is refused ===")
    tbif = parent_tbif()
    res = PandaTaskStepHandler(tbif).submit_target(make_parent_step("not-a-task"))
    failures += not check("not submitted", res.success is not True, res.success)
    failures += not check("and says why", "neither" in res.message, res.message)

    print("\n=== a parent whose step has not submitted is an inconsistency ===")
    tbif = parent_tbif()  # step 3 keeps target_id None
    res = PandaTaskStepHandler(tbif).submit_target(make_parent_step(PARENT_TASKID_PLACEHOLDER))
    failures += not check("not submitted", res.success is not True, res.success)
    failures += not check("and says the parent has no task", "no task yet" in res.message, res.message)

    print("\n=== setting a parent without noWaitParent is warned about ===")
    del LOGGED["warning"][:]
    tbif = parent_tbif()
    tbif.steps[3].target_id = "48810699"
    res = PandaTaskStepHandler(tbif).submit_target(make_parent_step(PARENT_TASKID_PLACEHOLDER, noWaitParent=False))
    failures += not check("still submitted", res.success is True, res.message)
    failures += not check("with a warning naming noWaitParent", any("noWaitParent" in m for m in LOGGED["warning"]), LOGGED["warning"])

    print("\n=== ${PARENT_TASKID:<step name>} on a joining step ===")

    def joining_tbif():
        data = {
            "left/OUT": FakeData("left/OUT", "a", source_step_id=3),
            "right/OUT": FakeData("right/OUT", "b", source_step_id=4),
            # the step's job parameters still reference this one, which input_data_dict does not
            # list, so it takes no part in working out the parent
            "merge_evnt/EVNT": FakeData("merge_evnt/EVNT", produced_evnt, source_step_id=None),
            "simul/HITS": FakeData("simul/HITS", f"...tid{TASKID_PLACEHOLDER}_00"),
        }
        left, right = FakeStep({}, None), FakeStep({}, None)
        left.name, left.step_id, left.target_id = "left", 3, "52000003"
        right.name, right.step_id, right.target_id = "right", 4, "52000004"
        return FakeTaskBuffer(data_by_name=data, steps_by_id={3: left, 4: right})

    # `expected` already names a bool | None earlier in this function
    for named, expected_task in (("left", 52000003), ("right", 52000004)):
        tbif = joining_tbif()
        res = PandaTaskStepHandler(tbif).submit_target(make_parent_step(f"${{PARENT_TASKID:{named}}}", inputs=("left/OUT", "right/OUT")))
        failures += not check(f"naming {named} submits", res.success is True, res.message)
        failures += not check(f"...with its task {expected_task}", tbif.inserted_parent_tids == [expected_task], tbif.inserted_parent_tids)

    print("\n=== naming a step that does not feed this one ===")
    tbif = joining_tbif()
    res = PandaTaskStepHandler(tbif).submit_target(make_parent_step("${PARENT_TASKID:elsewhere}", inputs=("left/OUT", "right/OUT")))
    failures += not check("is refused", res.success is not True, res.success)
    failures += not check("naming what does feed it", "left" in res.message and "right" in res.message, res.message)
    failures += not check("nothing inserted", tbif.inserted == [], tbif.inserted)

    print("\n=== the bare form on a join now points at the named form ===")
    tbif = joining_tbif()
    res = PandaTaskStepHandler(tbif).submit_target(make_parent_step(PARENT_TASKID_PLACEHOLDER, inputs=("left/OUT", "right/OUT")))
    failures += not check("still refused", res.success is not True, res.success)
    failures += not check("and says how to disambiguate", "PARENT_TASKID:<step name>" in res.message, res.message)

    print("\n=== naming the only feeding step is the same as the bare form ===")
    tbif = parent_tbif()
    tbif.steps[3].target_id = "48810699"
    res = PandaTaskStepHandler(tbif).submit_target(make_parent_step("${PARENT_TASKID:merge_evnt}"))
    failures += not check("submits with that task", res.success is True and tbif.inserted_parent_tids == [48810699], tbif.inserted_parent_tids)

    print("\n=== check_target status mapping ===")
    expectations = {
        WFStepStatus.running: ["running", "scouting", "scouted", "throttled", "prepared", "finishing", "passed", "merging", "toretry", "toincexec", "paused"],
        WFStepStatus.starting: [
            "registered",
            "defined",
            "assigned",
            "activated",
            "starting",
            "ready",
            "topreprocess",
            "preprocessing",
            "staging",
            "staged",
            "rerefine",
        ],
        WFStepStatus.done: ["done", "finished"],
        WFStepStatus.failed: ["failed", "exhausted", "aborted", "toabort", "aborting", "broken", "tobroken"],
    }
    for expected_status, statuses in expectations.items():
        for status in statuses:
            tbif = make_tbif()
            tbif.set_status(status)
            handler = PandaTaskStepHandler(tbif)
            step = make_step()
            step.status = WFStepStatus.running
            step.target_id = "49900001"
            result = handler.check_target(step)
            if not (result.success and result.step_status == expected_status):
                failures += not check(f"{status} -> {expected_status}", False, f"got success={result.success} status={result.step_status}")
    failures += not check(f"all {sum(len(v) for v in expectations.values())} task statuses map without error", True)
    # an unknown status must still be reported rather than silently mapped
    tbif = make_tbif()
    tbif.set_status("nonsense_status")
    handler = PandaTaskStepHandler(tbif)
    step = make_step()
    step.status = WFStepStatus.running
    step.target_id = "49900001"
    result = handler.check_target(step)
    failures += not check("an unknown status is still an error", result.success is False and "unknown" in result.message, result.message)

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
