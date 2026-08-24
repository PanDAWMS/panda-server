"""
Offline check of insert_step_task, the DEFT insert used for a workflow step's task.

Drives the real WorkflowModule.insert_step_task against a fake cursor, so the SQL it builds, the
late-bound ${TASKID} resolution, the active-task throttle and both sequence backends are exercised
without a database.

Run from the repository root:  python3 pandaserver/workflow/examples/insert_step_task_test.py
"""

import json
import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))


def stub(n, **a):
    m = types.ModuleType(n)
    [setattr(m, k, v) for k, v in a.items()]
    sys.modules[n] = m
    return m


stub("pandacommon")
pu = stub("pandacommon.pandautils")
pu.__path__ = []
stub("pandacommon.pandautils.base", SpecBase=object)


def get_sql_IN_bind_variables(vals, prefix=":", value_as_suffix=False):
    names = [f"{prefix}{v}" for v in vals]
    return ",".join(names), {n: v for n, v in zip(names, vals)}


stub("pandacommon.pandautils.PandaUtils", get_sql_IN_bind_variables=get_sql_IN_bind_variables, naive_utcnow=lambda: None)
pl = stub("pandacommon.pandalogger")
pl.__path__ = []


class Log:
    def __init__(s, *a, **k):
        s.msgs = []

    def info(s, m):
        pass

    def debug(s, m):
        s.msgs.append(m)

    def warning(s, m):
        pass

    def error(s, m):
        s.msgs.append("ERR:" + str(m))


stub("pandacommon.pandalogger.LogWrapper", LogWrapper=Log)
stub("pandacommon.pandalogger.PandaLogger", PandaLogger=lambda: types.SimpleNamespace(getLogger=lambda n: None))
stub("pandaserver.config", panda_config=types.SimpleNamespace(schemaJEDI="ATLAS_PANDA", schemaDEFT="ATLAS_DEFT"))
stub("pandaserver.srvcore")
stub("pandaserver.srvcore.CoreUtils", clean_user_id=lambda d: "tester")
stub("pandaserver.taskbuffer.ErrorCode")
stub("pandaserver.taskbuffer.JobUtils")
stub("pandaserver.taskbuffer.JobSpec", JobSpec=object)


class BaseModule:
    def __init__(self, log_stream=None):
        self.backend = "oracle"

    def create_tagged_logger(self, comment, tag=""):
        return Log()

    def getvalue_corrector(self, v):
        return v

    def getConfigValue(self, comp, key, app=None, vo=None):
        return self._max

    def dump_error_message(self, log):
        pass

    def _commit(self):
        return True

    def _rollback(self):
        pass


stub("pandaserver.taskbuffer.db_proxy_mods.base_module", BaseModule=BaseModule, varNUMBER=int)
stub("pandaserver.taskbuffer.db_proxy_mods.entity_module", get_entity_module=lambda s: None)

import contextlib

from pandaserver.taskbuffer.db_proxy_mods.workflow_module import WorkflowModule
from pandaserver.workflow.workflow_base import TASKID_PLACEHOLDER


class Cur:
    def __init__(s, task_id, active_count):
        s.task_id = task_id
        s.active_count = active_count
        s.executed = []
        s._last = None

    def var(s, t):
        return "OUT"

    def execute(s, sql, varmap=None):
        s.executed.append((sql, dict(varmap or {})))
        if "COUNT(*)" in sql:
            s._last = (s.active_count,)

    def fetchone(s):
        return s._last

    def getvalue(s, ref):
        return s.task_id


class Proxy(WorkflowModule):
    def __init__(s, task_id=49900001, active_count=0, max_tasks=None):
        s.backend = "oracle"
        s.cur = Cur(task_id, active_count)
        s._max = max_tasks

    def create_tagged_logger(s, comment, tag=""):
        return Log()

    def getvalue_corrector(s, v):
        return v

    def getConfigValue(s, comp, key, app=None, vo=None):
        return s._max

    def dump_error_message(s, log):
        pass

    def _commit(s):
        return True

    def _rollback(s):
        pass

    @contextlib.contextmanager
    def transaction(s, name=None, tmp_log=None):
        yield (s.cur, tmp_log or Log())


FAILURES = []


def ok(label, condition, detail=""):
    if not condition:
        FAILURES.append(label)
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")


base = {
    "taskName": "mc23.evgen.e8590",
    "vo": "atlas",
    "prodSourceLabel": "managed",
    "userName": "mnegrini",
    "taskPriority": 275,
    "jobParameters": [
        {
            "type": "template",
            "param_type": "output",
            "dataset": f"mc23.evgen.EVNT.e8590_wfid12345_tid{TASKID_PLACEHOLDER}_00",
            "value": f"--outputEVNTFile=EVNT.{TASKID_PLACEHOLDER}._${{SN}}.pool.root",
        }
    ],
}

print("insert_step_task:")
p = Proxy()
tid, msg = p.insert_step_task(dict(base), "/DC=ch/CN=t")
ok("returns the new task id", tid == 49900001, (tid, msg))
sqls = [e[0] for e in p.cur.executed]
ok("one INSERT into T_TASK", sum("INSERT INTO ATLAS_DEFT.T_TASK" in q for q in sqls) == 1, sqls)
ok("followed by the placeholder UPDATE", sum("UPDATE ATLAS_DEFT.T_TASK SET jedi_task_parameters" in q for q in sqls) == 1)
ins = [e for e in p.cur.executed if "INSERT INTO ATLAS_DEFT.T_TASK" in e[0]][0]
ok("uses the sequence for taskid", "PRODSYS2_TASK_ID_SEQ.nextval" in ins[0])
ok("own id as parent when no parent given", "PRODSYS2_TASK_ID_SEQ.currval" in ins[0])
ok("priority carried from taskPriority", ins[1][":priority"] == 275 and ins[1][":current_priority"] == 275)
ok("userName taken as authored, not the DN", ins[1][":userName"] == "mnegrini")
ok("status queued as waiting", ins[1][":status"] == "waiting")
ok("inserted params still hold the placeholder", TASKID_PLACEHOLDER in ins[1][":param"])
upd = [e for e in p.cur.executed if "UPDATE" in e[0]][0]
ok("update resolves it to the real id", TASKID_PLACEHOLDER not in upd[1][":param"] and "tid49900001_00" in upd[1][":param"])
ok("update also resolves it in the LFN template", "EVNT.49900001._${SN}" in upd[1][":param"])
ok("update targets the new task", upd[1][":taskid"] == 49900001)

print("\nno placeholder -> no extra UPDATE:")
clean = json.loads(json.dumps(base).replace(TASKID_PLACEHOLDER, "48810693"))
p2 = Proxy()
tid2, _ = p2.insert_step_task(clean, "/DC=ch/CN=t")
ok("still inserts", tid2 == 49900001)
ok("no UPDATE issued", not any("UPDATE" in q for q, _ in p2.cur.executed))

print("\nexplicit parent_tid:")
p3 = Proxy()
p3.insert_step_task(dict(base), "/DC=ch/CN=t", parent_tid=48810693)
ins3 = [e for e in p3.cur.executed if "INSERT INTO" in e[0]][0]
ok("binds the given parent", ins3[1][":parent_tid"] == 48810693 and "currval" not in ins3[0])

print("\nactive task throttle:")
p4 = Proxy(active_count=500, max_tasks=100)
tid4, msg4 = p4.insert_step_task(dict(base), "/DC=ch/CN=t")
ok("refused when over the limit", tid4 is None)
ok("message explains it", "too many active tasks" in msg4, msg4)
ok("nothing inserted", not any("INSERT INTO ATLAS_DEFT.T_TASK" in q for q, _ in p4.cur.executed))
p5 = Proxy(active_count=50, max_tasks=100)
tid5, _ = p5.insert_step_task(dict(base), "/DC=ch/CN=t")
ok("allowed when under the limit", tid5 == 49900001)
p6 = Proxy(active_count=999, max_tasks=None)
tid6, _ = p6.insert_step_task(dict(base), "/DC=ch/CN=t")
ok("no limit configured -> no throttle query", tid6 == 49900001 and not any("COUNT(*)" in q for q, _ in p6.cur.executed))

print("\nmissing required params:")
for key in ["taskName", "vo", "prodSourceLabel", "userName"]:
    bad = dict(base)
    del bad[key]
    t, m = Proxy().insert_step_task(bad, "/DC=ch/CN=t")
    ok(f"rejects missing {key}", t is None and key in m, m)

print("\nmysql-style backend (no sequence):")


class MyProxy(Proxy):
    def __init__(s, **k):
        super().__init__(**k)
        s.backend = "mysql"


class MyCur(Cur):
    def execute(s, sql, varmap=None):
        s.executed.append((sql, dict(varmap or {})))
        if "COUNT(*)" in sql:
            s._last = (s.active_count,)
        elif "LAST_INSERT_ID" in sql:
            s._last = (49900001,)


p7 = MyProxy()
p7.cur = MyCur(49900001, 0)
tid7, msg7 = p7.insert_step_task(dict(base), "/DC=ch/CN=t")
ok("inserts via the stand-in sequence table", tid7 == 49900001, msg7)
q7 = [e for e in p7.cur.executed if "INSERT INTO ATLAS_DEFT.T_TASK" in e[0]][0]
ok("binds nextval instead of a sequence", q7[1].get(":nextval") == 49900001 and "PRODSYS2_TASK_ID_SEQ.nextval" not in q7[0])
ok("own id as parent uses the same value", ":nextval" in q7[0].split("VALUES")[1] and "currval" not in q7[0])

print(f"\n{'ALL CHECKS PASSED' if not FAILURES else f'{len(FAILURES)} CHECK(S) FAILED'}")
sys.exit(1 if FAILURES else 0)
