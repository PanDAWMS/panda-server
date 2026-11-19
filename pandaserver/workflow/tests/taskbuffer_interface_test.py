"""
Check that every task buffer method the workflow code calls exists on TaskBuffer at runtime.

The other harnesses drive the workflow against a fake task buffer, which implements whatever the
code happens to call -- so a method TaskBuffer cannot serve is invisible to them. That is exactly how
on_all_inputs_done shipped calling updateTask_JEDI, release_task_on_hold and push_task_trigger_message,
none of which TaskBuffer exposed then: the hook raised AttributeError inside its own except clause on
every cycle, so a workflow step's holdup was never released and the chain stalled with the error only
visible in the handler log.

TaskBuffer serves the methods it defines itself, and its __getattr__ forwards every other public
method of the DB proxy class through the pool (pandaserver/taskbuffer/tests/forwarding_tests.py
checks that rule at runtime). Any other name is an AttributeError. This test parses the sources (no
DB, no imports), builds the same set of names and compares.

Run from the repository root:  python3 pandaserver/workflow/tests/taskbuffer_interface_test.py
"""

import ast
import os
import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
TASK_BUFFER = REPO_ROOT / "pandaserver/taskbuffer/TaskBuffer.py"
# the DB proxy class, and the directory of the modules its bases come from
DB_PROXY = REPO_ROOT / "pandaserver/taskbuffer/OraDBProxy.py"
DB_PROXY_MODS = REPO_ROOT / "pandaserver/taskbuffer/db_proxy_mods"
# the workflow sources whose self.tbif calls must be satisfied by TaskBuffer
WORKFLOW_SOURCES = [
    "pandaserver/workflow/workflow_core.py",
    "pandaserver/workflow/step_handler_plugins/panda_task_step_handler.py",
    "pandaserver/workflow/step_handler_plugins/base_step_handler.py",
    "pandaserver/workflow/data_handler_plugins/panda_task_data_handler.py",
    "pandaserver/workflow/data_handler_plugins/ddm_collection_data_handler.py",
    "pandaserver/workflow/data_handler_plugins/base_data_handler.py",
]


def is_type_checking(test: ast.expr) -> bool:
    return isinstance(test, ast.Name) and test.id == "TYPE_CHECKING"


def runtime_functions(body: list[ast.stmt]) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    """The functions a class body defines when it runs, also those under if blocks.

    An `if TYPE_CHECKING:` branch is skipped and an `if not TYPE_CHECKING:` branch is taken, as at
    runtime. Functions nested inside functions are not class attributes and are left out.
    """
    found: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    for node in body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            found.append(node)
        elif isinstance(node, ast.If):
            if is_type_checking(node.test):
                found += runtime_functions(node.orelse)
            elif isinstance(node.test, ast.UnaryOp) and isinstance(node.test.op, ast.Not) and is_type_checking(node.test.operand):
                found += runtime_functions(node.body)
            else:
                found += runtime_functions(node.body) + runtime_functions(node.orelse)
    return found


def is_property(func: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """A property is not a function on the class, so __getattr__ does not forward it."""
    for dec in func.decorator_list:
        name = dec.attr if isinstance(dec, ast.Attribute) else dec.id if isinstance(dec, ast.Name) else None
        if name in ("property", "cached_property", "setter", "getter", "deleter"):
            return True
    return False


def task_buffer_class() -> ast.ClassDef:
    tree = ast.parse(TASK_BUFFER.read_text())
    return next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "TaskBuffer")


def task_buffer_methods() -> set[str]:
    return {f.name for f in runtime_functions(task_buffer_class().body)}


def has_getattr_delegation() -> bool:
    return "__getattr__" in task_buffer_methods()


def db_proxy_public_methods() -> set[str]:
    """The public functions of the DB proxy class and of its bases: what TaskBuffer.__getattr__ forwards."""
    classes: dict[str, ast.ClassDef] = {}
    for path in [DB_PROXY, *sorted(DB_PROXY_MODS.glob("*.py"))]:
        for node in ast.parse(path.read_text()).body:
            if isinstance(node, ast.ClassDef):
                if node.name in classes:
                    raise RuntimeError(f"class {node.name} is defined twice, in {path} too")
                classes[node.name] = node
    names: set[str] = set()
    pending, seen = ["DBProxy"], set()
    while pending:
        class_name = pending.pop()
        if class_name in seen or class_name == "object":
            continue
        seen.add(class_name)
        if class_name not in classes:
            raise RuntimeError(f"base class {class_name} of the DB proxy is not in {DB_PROXY} or {DB_PROXY_MODS}")
        cls = classes[class_name]
        names |= {f.name for f in runtime_functions(cls.body) if not f.name.startswith("_") and not is_property(f)}
        for base in cls.bases:
            pending.append(base.attr if isinstance(base, ast.Attribute) else base.id if isinstance(base, ast.Name) else ast.unparse(base))
    return names


def called_methods(path: pathlib.Path) -> dict[str, list[int]]:
    """Map method name -> sorted line numbers, for every self.tbif.<name>( call."""
    calls: dict[str, list[int]] = {}
    for lineno, line in enumerate(path.read_text().splitlines(), start=1):
        for name in re.findall(r"self\.tbif\.([A-Za-z_][A-Za-z0-9_]*)\s*\(", line):
            calls.setdefault(name, []).append(lineno)
    return calls


def main():
    failures = 0
    own = task_buffer_methods()
    forwarded = db_proxy_public_methods() - own if has_getattr_delegation() else set()
    available = own | forwarded
    print(f"\nTaskBuffer defines {len(own)} methods and forwards {len(forwarded)} more of the DB proxy through __getattr__")

    total = 0
    for rel in WORKFLOW_SOURCES:
        path = REPO_ROOT / rel
        if not path.exists():
            print(f"  FAIL  {rel} does not exist")
            failures += 1
            continue
        calls = called_methods(path)
        total += len(calls)
        missing = {n: ls for n, ls in calls.items() if n not in available}
        status = "PASS" if not missing else "FAIL"
        print(f"  {status}  {rel}  ({len(calls)} distinct calls)")
        for name, lines in sorted(missing.items()):
            print(f"          MISSING on TaskBuffer and the DB proxy: {name}  (called at line{'s' if len(lines) > 1 else ''} {', '.join(map(str, lines))})")
            failures += 1

    print(f"\nchecked {total} distinct self.tbif call sites across {len(WORKFLOW_SOURCES)} modules")

    # the three that were missing, called by on_all_inputs_done -- named explicitly so a revert is caught
    print("\nthe methods whose absence stalled workflow 130:")
    for name in ("updateTask_JEDI", "release_task_on_hold", "push_task_trigger_message"):
        present = name in available
        where = "TaskBuffer" if name in own else "DB proxy, through __getattr__" if name in forwarded else "neither"
        print(f"  {'PASS' if present else 'FAIL'}  {name}  ({where})")
        if not present:
            failures += 1

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
