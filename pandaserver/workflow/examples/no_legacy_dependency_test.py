"""
Offline check that the native workflow engine does not depend on the legacy workflow modules.

The native engine and the legacy iDDS-backed workflow processor
(pandaserver.taskbuffer.workflow_processor) are meant to be mutually independent: the processor owns
pcwl_utils, snakeparser and workflow_utils, and through workflow_utils it owns the idds dependency.
Nothing in the native engine may reach any of them, which is what lets the engine run on a
deployment with no idds and no snakemake installed.

The check imports every native module with only the third-party packages the native path
legitimately needs stubbed out, and watches every import that goes past. idds and snakemake are
deliberately left unavailable, and any attempt to import them is recorded even when the importer
wraps it in try/except ImportError, so a coupling reintroduced anywhere in the import graph is
caught here rather than on a deployment that has neither installed.

Run from the repository root:  python3 pandaserver/workflow/examples/no_legacy_dependency_test.py
"""

import importlib
import importlib.abc
import importlib.machinery
import os
import sys
import types
import warnings

# The import graph reaches unrelated modules that still carry unescaped regex literals. Their
# SyntaxWarnings say nothing about this check, so they are filtered to keep the output readable.
warnings.filterwarnings("ignore", category=SyntaxWarning)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, REPO_ROOT)

# Native modules that must stand on their own
NATIVE_MODULES = (
    "pandaserver.workflow.workflow_base",
    "pandaserver.workflow.workflow_native_utils",
    "pandaserver.workflow.workflow_parser",
    "pandaserver.workflow.workflow_core",
    "pandaserver.workflow.data_handler_plugins.base_data_handler",
    "pandaserver.workflow.data_handler_plugins.panda_task_data_handler",
    "pandaserver.workflow.data_handler_plugins.ddm_collection_data_handler",
    "pandaserver.workflow.step_handler_plugins.base_step_handler",
    "pandaserver.workflow.step_handler_plugins.panda_task_step_handler",
)

# Third-party packages the native path may legitimately import. idds and snakemake are absent on
# purpose: they must never be imported, so leaving them unstubbed turns a coupling into a failure.
STUB_ROOTS = ("pandaclient", "ruamel", "requests", "rucio")

# Modules whose presence in sys.modules means a legacy dependency crept back in
FORBIDDEN_ROOTS = ("idds", "snakemake")
FORBIDDEN_MODULES = (
    "pandaserver.workflow.workflow_utils",
    "pandaserver.workflow.pcwl_utils",
    "pandaserver.workflow.snakeparser",
    "pandaserver.taskbuffer.workflow_processor",
)


# Every attempt to import a forbidden package, recorded here so that one suppressed by a
# try/except ImportError in the importing module is still reported
FORBIDDEN_ATTEMPTS = []


class ForbiddenWatcher(importlib.abc.MetaPathFinder):
    """Records attempts to import a forbidden package, then lets the import fail as normal"""

    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in FORBIDDEN_ROOTS:
            FORBIDDEN_ATTEMPTS.append(name)
        return None


class AutoStubFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in STUB_ROOTS:
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
sys.meta_path.insert(0, ForbiddenWatcher())


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

    debug = info
    warning = info
    error = info


stub("pandacommon")
stub("pandacommon.pandautils").__path__ = []
stub("pandacommon.pandautils.base", SpecBase=object)
stub("pandacommon.pandautils.PandaUtils", naive_utcnow=lambda: None, get_sql_IN_bind_variables=lambda *a, **k: (None, None))
stub("pandacommon.pandautils.thread_utils", GenericThread=object)
stub("pandacommon.pandalogger").__path__ = []
stub("pandacommon.pandalogger.LogWrapper", LogWrapper=Log)
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda: types.SimpleNamespace(getLogger=lambda n: None))
stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def main():
    failures = 0

    print("\n=== every native module imports without idds or snakemake ===")
    for module_name in NATIVE_MODULES:
        try:
            importlib.import_module(module_name)
            failures += not check(module_name, True)
        except Exception as exc:
            failures += not check(module_name, False, f"{type(exc).__name__}: {exc}")

    print("\n=== nothing legacy was pulled into the import graph ===")
    reached_roots = sorted({name.split(".")[0] for name in sys.modules} & set(FORBIDDEN_ROOTS))
    failures += not check("no idds or snakemake package imported", not reached_roots, reached_roots)
    failures += not check("no idds or snakemake import even attempted", not FORBIDDEN_ATTEMPTS, sorted(set(FORBIDDEN_ATTEMPTS)))
    for forbidden in FORBIDDEN_MODULES:
        reached = sorted(name for name in sys.modules if name == forbidden or name.startswith(forbidden + "."))
        failures += not check(f"{forbidden} not imported", not reached, reached)

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
