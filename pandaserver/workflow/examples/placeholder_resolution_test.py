"""
Offline check of the late-bound ID placeholders in a workflow description.

${TASKID} is used by the production example, since a dataset name embeds the ID of the task that
produced it. ${WFID} is supported but deliberately unused there: ATLAS dataset names follow a
convention that carries no workflow ID. This test therefore exercises ${WFID} against a synthetic
description, so the capability keeps working should a future convention want it.

Run from the repository root:  python3 pandaserver/workflow/examples/placeholder_resolution_test.py
"""

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


class Log:
    def __init__(self, *a, **k):
        pass

    def info(self, m):
        pass

    def debug(self, m):
        pass

    def warning(self, m):
        print(f"  WARN: {m}")

    def error(self, m):
        print(f"  ERROR: {str(m)[:200]}")


stub("pandacommon")
stub("pandacommon.pandautils").__path__ = []
stub("pandacommon.pandautils.base", SpecBase=object)
stub("pandacommon.pandalogger").__path__ = []
stub("pandacommon.pandalogger.LogWrapper", LogWrapper=Log)
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda: types.SimpleNamespace(getLogger=lambda n: None))
stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))

from pandaserver.workflow.workflow_base import (  # noqa: E402
    TASKID_PLACEHOLDER,
    WFID_PLACEHOLDER,
    has_placeholder,
    substitute_placeholder,
)
from pandaserver.workflow.workflow_parser import (  # noqa: E402
    INLINE_DESCRIPTION_KEY,
    parse_raw_request,
)

# A synthetic description using BOTH placeholders, including ${WFID} which the production example
# deliberately does not use. Kept minimal: one step, one output.
DESCRIPTION = {
    "name": "placeholder_probe",
    "outputs": {"out": {"from": "only_step/EVNT"}},
    "steps": {
        "only_step": {
            "type": "task",
            "task_params": {
                "taskName": "probe.evgen.e0001",
                "taskType": "prod",
                "prodSourceLabel": "managed",
                "vo": "atlas",
                "userName": "prober",
                "transPath": "Gen_tf.py",
                "noInput": True,
                "log": {
                    "type": "template",
                    "param_type": "log",
                    "value": "log.${TASKID}._${SN}.job.log.tgz",
                    "dataset": "probe.evgen.log.e0001_wfid${WFID}_tid${TASKID}_00",
                },
                "jobParameters": [
                    {
                        "type": "template",
                        "param_type": "output",
                        "value": "--outputEVNTFile=EVNT.${TASKID}._${SN}.pool.root",
                        "dataset": "probe.evgen.EVNT.e0001_wfid${WFID}_tid${TASKID}_00",
                    }
                ],
            },
        }
    },
}


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def main():
    failures = 0

    print("\n=== the helpers ===")
    both = "ds.e0001_wfid${WFID}_tid${TASKID}_00"
    after_wfid = substitute_placeholder(both, WFID_PLACEHOLDER, 10)
    after_both = substitute_placeholder(after_wfid, TASKID_PLACEHOLDER, 4004198)
    failures += not check("WFID resolves, TASKID left alone", after_wfid == "ds.e0001_wfid10_tid${TASKID}_00", after_wfid)
    failures += not check("TASKID then resolves", after_both == "ds.e0001_wfid10_tid4004198_00", after_both)
    failures += not check("has_placeholder tracks both", has_placeholder(both, WFID_PLACEHOLDER) and not has_placeholder(after_wfid, WFID_PLACEHOLDER))
    failures += not check("substitution recurses into nested structures", substitute_placeholder({"a": ["x${WFID}"]}, WFID_PLACEHOLDER, 7) == {"a": ["x7"]})
    failures += not check("JEDI per-job templates untouched", substitute_placeholder("log.${TASKID}._${SN}.tgz", TASKID_PLACEHOLDER, 7) == "log.7._${SN}.tgz")

    print("\n=== ${WFID} is resolved when the description is parsed ===")
    is_ok, is_fatal, definition = parse_raw_request(None, "<probe>", "prober", {INLINE_DESCRIPTION_KEY: DESCRIPTION}, workflow_id=4242)
    failures += not check("parsed", is_ok and not is_fatal)
    blob = json.dumps(definition, default=str)
    failures += not check("${WFID} resolved to the workflow id", "${WFID}" not in blob and "wfid4242" in blob)
    failures += not check("${TASKID} deliberately left for submission time", "${TASKID}" in blob)
    node = definition["nodes"][0]
    print(f"    output dataset: {node['outputs']['only_step/EVNT']['value']}")
    print(f"    log dataset:    {node['task_params']['log']['dataset']}")
    failures += not check(
        "resolved in the output dataset name",
        node["outputs"]["only_step/EVNT"]["value"] == "probe.evgen.EVNT.e0001_wfid4242_tid${TASKID}_00",
        node["outputs"]["only_step/EVNT"]["value"],
    )
    failures += not check(
        "resolved in the log dataset name too",
        node["task_params"]["log"]["dataset"] == "probe.evgen.log.e0001_wfid4242_tid${TASKID}_00",
        node["task_params"]["log"]["dataset"],
    )

    print("\n=== without a workflow id the placeholder is left in place ===")
    is_ok2, _, definition2 = parse_raw_request(None, "<probe>", "prober", {INLINE_DESCRIPTION_KEY: DESCRIPTION}, workflow_id=None)
    failures += not check("parsed", is_ok2)
    failures += not check("${WFID} untouched", "${WFID}" in json.dumps(definition2, default=str))

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
