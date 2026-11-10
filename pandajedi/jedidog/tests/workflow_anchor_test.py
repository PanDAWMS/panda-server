"""
Offline check of how a native workflow's brokerage anchor is computed and stored.

The anchor is what lets a step with no input of its own learn where the rest of its workflow has
to run. Two properties matter beyond the arithmetic, and both are the kind that only show up in
production: it has to exist before the first step submits a task, and storing it must not clobber
what the engine writes to the same JSON column.

Run from the repository root:  python3 pandajedi/jedidog/tests/workflow_anchor_test.py
"""

import datetime
import importlib.abc
import importlib.machinery
import json
import os
import sys
import types
import warnings

warnings.filterwarnings("ignore", category=SyntaxWarning)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

AUTO_STUB_ROOTS = ("pandacommon", "idds", "pandaclient", "ruamel", "requests", "rucio", "numpy")


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


class Log:
    def __getattr__(self, name):
        return lambda *a, **k: None


stub("pandacommon.pandalogger").__path__ = []
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda *a, **k: types.SimpleNamespace(getLogger=lambda n: Log()))
stub("pandacommon.pandautils").__path__ = []
stub("pandacommon.pandautils.base", SpecBase=object)
stub("pandacommon.pandautils.PandaUtils", naive_utcnow=datetime.datetime.utcnow, get_sql_IN_bind_variables=lambda *a, **k: (None, None))


class _AnyConfig:
    """Answers any attribute, so importing a module that reads a cfg file does not need one"""

    def __getattr__(self, name):
        return _AnyConfig()

    def __bool__(self):
        return False


stub("pandaserver.config", panda_config=_AnyConfig())
stub("pandajedi.jediconfig", jedi_config=_AnyConfig())

from pandajedi.jedibrokerage.workflow_affinity import (  # noqa: E402
    WORKFLOW_ANCHOR_PARAM,
    build_anchor_weights,
)


class FakeWorkflowSpec:
    """A workflow row, with the parameters column behaving as the real spec's does"""

    def __init__(self, workflow_id, parameters=None):
        self.workflow_id = workflow_id
        self.status = "running"
        self.parameters = json.dumps(parameters) if parameters else None

    def get_parameter(self, name):
        return json.loads(self.parameters).get(name) if self.parameters else None

    def set_parameter(self, name, value):
        parameter_map = json.loads(self.parameters) if self.parameters else {}
        parameter_map[name] = value
        self.parameters = json.dumps(parameter_map)


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def main():
    failures = 0

    print("\n=== the motivating workflow ===")
    # s1 has no input, s2's input is at SITE_A, s3 takes both outputs. Nothing has run yet, so the
    # only knowledge is where s2's input sits -- and that is what has to reach s1.
    anchor = build_anchor_weights(input_sites=["SITE_A"])
    failures += not check("the site holding a sibling's input is an anchor", anchor == {"SITE_A": 1.0}, anchor)
    failures += not check("...before anything has run", "SITE_A" in anchor)

    print("\n=== a step that has already run pulls harder than a prediction ===")
    anchor = build_anchor_weights(output_sites=["SITE_B"], input_sites=["SITE_A"])
    failures += not check("where output already sits outweighs where input might go", anchor["SITE_B"] > anchor["SITE_A"], anchor)

    print("\n=== a pinned sibling is the strongest signal ===")
    anchor = build_anchor_weights(pinned_sites=["SITE_P"], output_sites=["SITE_B"], input_sites=["SITE_A"])
    failures += not check("the pinned site leads", max(anchor.items(), key=lambda item: item[1])[0] == "SITE_P", anchor)

    print("\n=== a workflow nothing is known about has no anchor ===")
    failures += not check("empty in, empty out", build_anchor_weights() == {})
    failures += not check("...and an empty anchor is not stored", not build_anchor_weights(input_sites=[]))

    print("\n=== storing the anchor keeps what the engine wrote ===")
    # parameters is one JSON column that the engine also uses, for steps_final_time and the
    # failure reason among others. Writing a blob read before the engine's own write would drop
    # them, and losing steps_final_time restarts the output-settle grace period.
    spec = FakeWorkflowSpec(1, {"steps_final_time": "2026-11-05T09:00:00", "fail_reason": "step x failed"})
    spec.set_parameter(WORKFLOW_ANCHOR_PARAM, {"weights": {"SITE_A": 1.0}, "computed_at": "2026-11-05T10:00:00"})
    failures += not check("the engine's grace timestamp survives", spec.get_parameter("steps_final_time") == "2026-11-05T09:00:00")
    failures += not check("so does its failure reason", spec.get_parameter("fail_reason") == "step x failed")
    failures += not check("and the anchor is there", spec.get_parameter(WORKFLOW_ANCHOR_PARAM)["weights"] == {"SITE_A": 1.0})

    print("\n=== a stale copy is what would lose them ===")
    # This is the bug the workflow lock exists to prevent: compute against a spec read earlier,
    # and whatever the engine added in between is gone.
    engine_view = FakeWorkflowSpec(2, {"steps_final_time": "2026-11-05T09:00:00"})
    stale_copy = FakeWorkflowSpec(2, {})
    stale_copy.set_parameter(WORKFLOW_ANCHOR_PARAM, {"weights": {"SITE_A": 1.0}})
    failures += not check(
        "a blob written from a stale read drops the engine's key",
        stale_copy.get_parameter("steps_final_time") is None and engine_view.get_parameter("steps_final_time") is not None,
    )

    print("\n=== freshness decides whether the lookups are repeated ===")
    from pandajedi.jedidog import AtlasWorkflowManagerWatchDog as wd

    watchdog = wd.AtlasWorkflowManagerWatchDog.__new__(wd.AtlasWorkflowManagerWatchDog)
    now = datetime.datetime.utcnow()
    failures += not check("a workflow with no anchor is never fresh", not watchdog.anchor_is_fresh(FakeWorkflowSpec(3)))
    failures += not check(
        "...nor one whose anchor has no timestamp",
        not watchdog.anchor_is_fresh(FakeWorkflowSpec(3, {WORKFLOW_ANCHOR_PARAM: {"weights": {"A": 1}}})),
    )
    just_now = FakeWorkflowSpec(3, {WORKFLOW_ANCHOR_PARAM: {"weights": {"A": 1}, "computed_at": now.isoformat()}})
    failures += not check("one just computed is fresh", watchdog.anchor_is_fresh(just_now))
    old = now - datetime.timedelta(seconds=wd.ANCHOR_REFRESH_SEC + 60)
    aged = FakeWorkflowSpec(3, {WORKFLOW_ANCHOR_PARAM: {"weights": {"A": 1}, "computed_at": old.isoformat()}})
    failures += not check("an aged one is recomputed", not watchdog.anchor_is_fresh(aged))
    broken = FakeWorkflowSpec(3, {WORKFLOW_ANCHOR_PARAM: {"weights": {"A": 1}, "computed_at": "not-a-time"}})
    failures += not check("an unparsable timestamp is recomputed rather than trusted", not watchdog.anchor_is_fresh(broken))

    print("\n=== the pass runs before the workflows are processed ===")
    # An analysis task is job-generated almost as soon as it is submitted, so an anchor computed
    # after the engine advances a workflow would arrive too late for the first step.
    import inspect

    source = inspect.getsource(wd.AtlasWorkflowManagerWatchDog.doAction)
    failures += not check(
        "doComputeWorkflowAnchors is called before doProcessWorkflows",
        source.index("doComputeWorkflowAnchors") < source.index("doProcessWorkflows"),
        source,
    )

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
