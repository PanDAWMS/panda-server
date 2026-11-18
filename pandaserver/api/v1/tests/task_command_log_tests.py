# Description: Unit tests for the task command log, TASKS_COMMANDLOG: recording the commands around
# the task API endpoints, the encoders, and the SQL the DB proxy issues when a command is registered
# for JEDI or taken by it. No database is used; the TaskBuffer and the cursor are fakes
import datetime
import inspect
import json
import logging
import unittest
from collections.abc import Callable
from typing import Any
from unittest import mock

from pandaserver.api.v1 import common, task_api
from pandaserver.api.v1.common import (
    COMMAND_ARGUMENT_SIZE_LIMIT,
    COMMAND_RESPONSE_FIELD_LIMIT,
    COMMAND_TASK_ID_PARAMETERS,
    MESSAGE_COMMAND_LOG,
    encode_command_parameters,
    encode_command_response,
    generate_response,
    log_task_commands,
    request_validation,
)
from pandaserver.srvcore import task_command_log
from pandaserver.srvcore.CoreUtils import clean_user_id
from pandaserver.taskbuffer.db_proxy_mods.task_complex_module import TaskComplexModule
from pandaserver.taskbuffer.db_proxy_mods.task_event_module import TaskEventModule

_logger = logging.getLogger("task_command_log_tests")

NOW = datetime.datetime(2026, 11, 17, 12, 0, 0)
KEY: task_command_log.TaskCommandKey = {"command_id": 42, "command_time": NOW}
DN = "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=someone/CN=123456/CN=Some One"
TASK_ID = 1234


class FakeRequest:
    # what request_validation reads of a PandaRequest
    def __init__(self, method: str = "POST", remote_host: str = "", remote_addr: str = "192.0.2.10") -> None:
        self.subprocess_env: dict[str, str] = {"REQUEST_METHOD": method, "SSL_CLIENT_S_DN": DN, "REMOTE_ADDR": remote_addr}
        if remote_host:
            self.subprocess_env["REMOTE_HOST"] = remote_host

    def get_remote_host(self) -> str:
        return self.subprocess_env.get("REMOTE_HOST", "")


class FakeTaskBuffer:
    # what run_logged_task_command uses of a TaskBuffer
    def __init__(self, key: Any = KEY, response_ok: bool = True) -> None:
        self.key = key
        self.response_ok = response_ok
        self.inserted: list[dict[str, Any]] = []
        self.responses: list[tuple[Any, dict[str, Any], bool]] = []

    def insert_task_command(self, task_ids: list[int], command: str, parameters: str, requester: str, prod_role: bool, client_addr: str | None) -> Any:
        self.inserted.append(
            {
                "task_ids": task_ids,
                "command": command,
                "parameters": json.loads(parameters),
                "requester": requester,
                "prod_role": prod_role,
                "client_addr": client_addr,
            }
        )
        if isinstance(self.key, Exception):
            raise self.key
        return self.key

    def set_task_command_response(self, command_key: Any, response: str, is_error: bool) -> bool:
        self.responses.append((command_key, json.loads(response), is_error))
        return self.response_ok


fake_task_buffer = FakeTaskBuffer()
# the key each endpoint below saw in current_task_command while it ran
seen_keys: list[Any] = []


@request_validation(_logger, secure=True, request_method="POST")
def change_thing(req: Any, task_id: int, value: int, blob: dict[str, Any] | None = None) -> dict[str, Any]:
    seen_keys.append(task_command_log.current_task_command.get())
    return generate_response(True, message="1 tasks changed", data={"value": value})


@request_validation(_logger, secure=True, request_method="POST")
def share_tasks(req: Any, task_id_list: list[int], share: str) -> dict[str, Any]:
    seen_keys.append(task_command_log.current_task_command.get())
    return generate_response(True)


@request_validation(_logger, secure=True, request_method="POST")
def fail_thing(req: Any, task_id: int) -> dict[str, Any]:
    seen_keys.append(task_command_log.current_task_command.get())
    raise RuntimeError("boom")


@request_validation(_logger, secure=True, request_method="GET")
def read_thing(req: Any, task_id: int) -> dict[str, Any]:
    seen_keys.append(task_command_log.current_task_command.get())
    return generate_response(True)


@request_validation(_logger, secure=True, request_method="POST")
def submit_thing(req: Any, task_parameters: dict[str, Any]) -> dict[str, Any]:
    seen_keys.append(task_command_log.current_task_command.get())
    return generate_response(True)


class TestRecordingAroundEndpoints(unittest.TestCase):
    def setUp(self) -> None:
        global fake_task_buffer
        fake_task_buffer = FakeTaskBuffer()
        seen_keys.clear()
        log_task_commands(__name__, lambda: fake_task_buffer)

    def tearDown(self) -> None:
        common._command_logging_modules.pop(__name__, None)

    def test_post_is_recorded_around_the_command(self) -> None:
        response = change_thing(FakeRequest(), task_id=TASK_ID, value=3)
        self.assertEqual(response, {"success": True, "message": "1 tasks changed", "data": {"value": 3}})
        self.assertEqual(
            fake_task_buffer.inserted,
            [
                {
                    "task_ids": [TASK_ID],
                    "command": "change_thing",
                    "parameters": {"value": 3, "blob": None},
                    "requester": clean_user_id(DN),
                    "prod_role": False,
                    "client_addr": "192.0.2.10",
                }
            ],
        )
        # the key is there while the command runs, and gone after
        self.assertEqual(seen_keys, [KEY])
        self.assertIsNone(task_command_log.current_task_command.get())
        self.assertEqual(fake_task_buffer.responses, [(KEY, {"success": True, "message": "1 tasks changed", "data": {"value": 3}}, False)])

    def test_host_name_is_preferred_to_the_address(self) -> None:
        change_thing(FakeRequest(remote_host="client.example.org"), task_id=TASK_ID, value=3)
        self.assertEqual(fake_task_buffer.inserted[0]["client_addr"], "client.example.org")

    def test_large_argument_is_omitted(self) -> None:
        blob = {"x": "a" * COMMAND_ARGUMENT_SIZE_LIMIT}
        change_thing(FakeRequest(), task_id=TASK_ID, value=3, blob=blob)
        size = len(json.dumps(blob).encode())
        self.assertEqual(fake_task_buffer.inserted[0]["parameters"], {"value": 3, "blob": {"omitted_bytes": size}})

    def test_one_row_per_task_of_a_list(self) -> None:
        share_tasks(FakeRequest(), task_id_list=[1, 2], share="Express")
        self.assertEqual(fake_task_buffer.inserted[0]["task_ids"], [1, 2])
        self.assertEqual(fake_task_buffer.inserted[0]["parameters"], {"share": "Express"})

    def test_command_is_refused_when_it_cannot_be_recorded(self) -> None:
        for key in (None, RuntimeError("ORA-00942: table or view does not exist")):
            with self.subTest(key=key):
                fake_task_buffer.key = key
                seen_keys.clear()
                response = change_thing(FakeRequest(), task_id=TASK_ID, value=3)
                self.assertEqual(response, {"success": False, "message": MESSAGE_COMMAND_LOG, "data": None})
                self.assertEqual(seen_keys, [])
                self.assertEqual(fake_task_buffer.responses, [])

    def test_exception_is_recorded_then_raised(self) -> None:
        with self.assertRaises(RuntimeError):
            fail_thing(FakeRequest(), task_id=TASK_ID)
        self.assertEqual(seen_keys, [KEY])
        self.assertIsNone(task_command_log.current_task_command.get())
        self.assertEqual(fake_task_buffer.responses, [(KEY, {"exception": "RuntimeError: boom"}, True)])

    def test_response_is_returned_when_it_cannot_be_recorded(self) -> None:
        fake_task_buffer.response_ok = False
        response = change_thing(FakeRequest(), task_id=TASK_ID, value=3)
        self.assertEqual(response, {"success": True, "message": "1 tasks changed", "data": {"value": 3}})

    def test_not_recorded(self) -> None:
        # a GET, an endpoint without a task, a request refused by validation
        read_thing(FakeRequest(method="GET"), task_id=str(TASK_ID))
        submit_thing(FakeRequest(), task_parameters={"taskName": "x"})
        refused = change_thing(FakeRequest(), task_id="not a number", value=3)
        self.assertFalse(refused["success"])
        self.assertEqual(fake_task_buffer.inserted, [])
        self.assertEqual(seen_keys, [None, None])

    def test_unregistered_module_is_not_recorded(self) -> None:
        common._command_logging_modules.pop(__name__)
        change_thing(FakeRequest(), task_id=TASK_ID, value=3)
        self.assertEqual(fake_task_buffer.inserted, [])
        self.assertEqual(seen_keys, [None])


class TestTaskApiCoverage(unittest.TestCase):
    def test_task_api_is_registered(self) -> None:
        self.assertIn(task_api.__name__, common._command_logging_modules)

    def test_only_submit_takes_no_task(self) -> None:
        # every POST endpoint of the task API is recorded, except those that do not name the task they act on
        post_endpoints = []
        for name, func in inspect.getmembers(task_api, inspect.isfunction):
            if func.__module__ != task_api.__name__ or not hasattr(func, "__wrapped__"):
                continue
            if inspect.getclosurevars(func).nonlocals.get("request_method") == "POST":
                post_endpoints.append(name)
        self.assertEqual(len(post_endpoints), 21)
        without_task = [name for name in post_endpoints if not set(inspect.signature(getattr(task_api, name)).parameters) & set(COMMAND_TASK_ID_PARAMETERS)]
        self.assertEqual(without_task, ["submit"])


class TestEncoders(unittest.TestCase):
    def test_parameters_leave_out_req_and_tasks(self) -> None:
        encoded = json.loads(encode_command_parameters({"req": object(), "task_id": 1, "task_id_list": [1], "jedi_task_id": 1, "soft": True}))
        self.assertEqual(encoded, {"soft": True})

    def test_parameter_json_cannot_hold_is_kept_as_str(self) -> None:
        encoded = json.loads(encode_command_parameters({"value": float("nan"), "when": NOW}))
        self.assertEqual(encoded, {"value": "nan", "when": str(NOW)})

    def test_response_is_shortened(self) -> None:
        response = generate_response(False, message="m" * (COMMAND_RESPONSE_FIELD_LIMIT + 10), data=list(range(1000)))
        encoded = json.loads(encode_command_response(response))
        self.assertEqual(encoded["success"], False)
        self.assertEqual(encoded["message"], "m" * COMMAND_RESPONSE_FIELD_LIMIT)
        self.assertEqual(encoded["data"], {"omitted_bytes": len(json.dumps(list(range(1000))).encode())})

    def test_other_response_is_kept_as_str(self) -> None:
        self.assertEqual(json.loads(encode_command_response("plain text")), {"response": "plain text"})


Answer = Callable[[str, dict[str, Any]], tuple[list[tuple[Any, ...]], int]]


class FakeCursor:
    def __init__(self, answer: Answer) -> None:
        self.answer = answer
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self.rows: list[tuple[Any, ...]] = []
        self.rowcount = 0
        self.arraysize = 0

    def execute(self, sql: str, binds: dict[str, Any] | None = None) -> None:
        self.executed.append((sql, dict(binds or {})))
        self.rows, self.rowcount = self.answer(sql, binds or {})

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self.rows)

    def __iter__(self) -> Any:
        return iter(self.rows)


def make_proxy(module_class: type, answer: Answer) -> Any:
    # a DB proxy module on a fake cursor, with the writes outside the command log mocked
    proxy = module_class(_logger)
    proxy.conn = mock.Mock()
    proxy.cur = FakeCursor(answer)
    proxy.get_db_utc_now = mock.Mock(return_value=NOW)
    for name in ("record_task_status_change", "push_task_status_message", "setDeftStatus_JEDI", "setSuperStatus_JEDI", "record_job_status_change"):
        setattr(proxy, name, mock.Mock())
    return proxy


def command_log_writes(proxy: Any) -> list[tuple[str, dict[str, Any]]]:
    return [(sql, binds) for sql, binds in proxy.cur.executed if "TASKS_COMMANDLOG" in sql]


class TestRegisteringForJedi(unittest.TestCase):
    def test_queued_supersedes_the_one_before(self) -> None:
        proxy = make_proxy(TaskComplexModule, lambda sql, binds: ([], 1))
        proxy.mark_task_command_queued(TASK_ID, KEY, "kill")
        writes = command_log_writes(proxy)
        self.assertEqual(len(writes), 2)
        superseded_sql, superseded_binds = writes[0]
        self.assertIn("command_id<>:command_id", superseded_sql)
        self.assertEqual(
            superseded_binds,
            {
                ":jediTaskID": TASK_ID,
                ":new_status": "superseded",
                ":old_status": "queued",
                ":now": NOW,
                ":reason": "superseded by kill",
                ":command_id": 42,
            },
        )
        self.assertEqual(
            writes[1][1],
            {":command_id": 42, ":jediTaskID": TASK_ID, ":command_time": NOW, ":new_status": "queued", ":old_status": "received", ":now": NOW},
        )

    def test_command_without_a_row_only_supersedes(self) -> None:
        proxy = make_proxy(TaskComplexModule, lambda sql, binds: ([], 1))
        proxy.mark_task_command_queued(TASK_ID, None, "resume")
        writes = command_log_writes(proxy)
        self.assertEqual(len(writes), 1)
        self.assertNotIn("command_id", writes[0][0])
        self.assertEqual(writes[0][1][":reason"], "superseded by resume")

    def test_send_command_marks_the_row_in_its_transaction(self) -> None:
        def answer(sql: str, binds: dict[str, Any]) -> tuple[list[tuple[Any, ...]], int]:
            if sql.startswith("SELECT status, prodSourceLabel"):
                return [("running", "managed")], 1
            return [], 1

        proxy = make_proxy(TaskEventModule, answer)
        proxy._check_command_allowed = mock.Mock(return_value=(True, ""))
        ret = proxy.sendCommandTaskPanda(TASK_ID, DN, True, "kill", properErrorCode=True, command_key=KEY)
        self.assertEqual(ret[0], 0)
        tables = [sql.split()[0] + " " + next(word for word in sql.split() if "." in word) for sql, _ in proxy.cur.executed]
        # the row is marked after the command is written, and before the commit
        self.assertEqual(tables[-2:], ["UPDATE ATLAS_PANDA.TASKS_COMMANDLOG", "UPDATE ATLAS_PANDA.TASKS_COMMANDLOG"])
        self.assertTrue(tables[-3].startswith("INSERT"))
        proxy.conn.commit.assert_called_once()

    def test_send_command_rolls_back_when_the_row_cannot_be_marked(self) -> None:
        def answer(sql: str, binds: dict[str, Any]) -> tuple[list[tuple[Any, ...]], int]:
            if "TASKS_COMMANDLOG" in sql:
                raise RuntimeError("ORA-00942: table or view does not exist")
            if sql.startswith("SELECT status, prodSourceLabel"):
                return [("running", "managed")], 1
            return [], 1

        proxy = make_proxy(TaskEventModule, answer)
        proxy._check_command_allowed = mock.Mock(return_value=(True, ""))
        proxy._rollback = mock.Mock()
        proxy.dump_error_message = mock.Mock()
        ret = proxy.sendCommandTaskPanda(TASK_ID, DN, True, "kill", properErrorCode=True, command_key=KEY)
        self.assertEqual(ret, (1, "failed to register command"))
        proxy.conn.commit.assert_not_called()
        proxy._rollback.assert_called_once()


class TestTakenByJedi(unittest.TestCase):
    def run_exec(self, command: str, task_status: str) -> Any:
        def answer(sql: str, binds: dict[str, Any]) -> tuple[list[tuple[Any, ...]], int]:
            if sql.startswith("SELECT comm_task,comm_cmd,comm_comment"):
                return [(TASK_ID, command, f"{command} by someone")], 1
            if sql.startswith("SELECT comm_cmd FROM"):
                return [(command,)], 1
            if sql.startswith("SELECT status,oldStatus,wallTimeUnit,splitRule"):
                return [(task_status, None, None, "")], 1
            if sql.startswith("UPDATE"):
                return [], 1
            return [], 0

        proxy = make_proxy(TaskComplexModule, answer)
        proxy.getTasksToExecCommand_JEDI("atlas", "managed")
        return proxy

    def test_accepted(self) -> None:
        proxy = self.run_exec("kill", "running")
        writes = command_log_writes(proxy)
        self.assertEqual(len(writes), 1)
        self.assertEqual(
            writes[0][1],
            {":jediTaskID": TASK_ID, ":new_status": "accepted", ":old_status": "queued", ":now": NOW, ":reason": "running -> aborting"},
        )
        comm_updates = [binds for sql, binds in proxy.cur.executed if sql.startswith("UPDATE") and "PRODSYS_COMM" in sql]
        self.assertEqual(comm_updates[0][":comm_cmd"], "killing")

    def test_rejected(self) -> None:
        proxy = self.run_exec("retry", "running")
        writes = command_log_writes(proxy)
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0][1][":new_status"], "rejected")
        self.assertEqual(writes[0][1][":reason"], "task in running")
        comm_updates = [binds for sql, binds in proxy.cur.executed if sql.startswith("UPDATE") and "PRODSYS_COMM" in sql]
        self.assertEqual(comm_updates[0][":comm_cmd"], "retry failed")


if __name__ == "__main__":
    unittest.main()
