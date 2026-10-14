# Description: Unit tests for the handler of the async grep requests. They run rg on log files
# written to a temporary directory, so they are skipped where rg is not installed
import gzip
import json
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from typing import Any
from unittest import mock

from pandaserver.asyncprocess import processor
from pandaserver.config import daemon_config, panda_config
from pandaserver.taskbuffer.db_proxy_mods.async_request_module import (
    SERVICE_JEDI,
    SERVICE_SERVER,
)

# the lines of a log, a record of several lines among them
RECORDS = [
    "2026-10-08 00:00:01,001 adder: DEBUG start job=7353014619\n",
    "2026-10-08 00:00:02,001 adder: ERROR failed job=7353014619\n",
    "Traceback (most recent call last):\n",
    '  File "/opt/ddm.py", line 522, in f\n',
    "    raise X\n",
    "ValueError: boom\n",
    "2026-10-08 00:00:03,001 adder: DEBUG other\n",
    "2026-10-08 00:00:04,001 adder: DEBUG done job=7353014619\n",
]


def offset(first: int, lines: list[str] = RECORDS) -> int:
    """Where line first of the lines starts in their file."""
    return len("".join(lines[:first]))


def record(first: int, end: int, lines: list[str] = RECORDS, head: bool = True, cut: bool = False) -> dict[str, Any]:
    """A record of whole_records made of lines first to end - 1 of the lines."""
    return {"offset": offset(first, lines), "head": head, "lines": [line.removesuffix("\n") for line in lines[first:end]], "cut": cut}


@unittest.skipUnless(shutil.which("rg"), "rg is not installed")
class TestHandleGrep(unittest.TestCase):
    def setUp(self) -> None:
        self.logdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.logdir)
        patcher = mock.patch.object(panda_config, "logdir", self.logdir, create=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _write(self, name: str, text: str) -> None:
        with open(os.path.join(self.logdir, name), "w") as f:
            f.write(text)

    def _grep(self, **params: Any) -> dict[str, Any]:
        """Run the handler on a request with these parameters, and give what it stored."""
        task_buffer = mock.MagicMock()
        row = {"request_id": "r1", "parameters": json.dumps(params)}
        processor._handle_grep(row, task_buffer, mock.MagicMock(), "host")
        args, kwargs = task_buffer.finish_async_result.call_args
        return {"status": args[2], **kwargs}

    def test_pattern_starting_with_a_dash_is_searched_for(self) -> None:
        # in the place of an argument of its own, rg would read it as --pre and run the command
        # on every file under the working directory
        self._write("panda-a.log", "2026-10-08 00:00:01 opts --pre=true\n2026-10-08 00:00:02 other\n")
        out = self._grep(pattern="--pre=true", log_filename="panda-a.log")
        self.assertEqual(out["status"], "done")
        self.assertEqual(out["result"], "2026-10-08 00:00:01 opts --pre=true\n")

    def test_pattern_means_the_same_in_a_gz_file(self) -> None:
        # "+" repeats in rg; zgrep read it as a plain character in its basic regular expressions
        text = "2026-10-08 00:00:01 aaab\n2026-10-08 00:00:02 a+b\n"
        self._write("panda-a.log", text)
        with gzip.open(os.path.join(self.logdir, "panda-a.log-20261008.gz"), "wt") as f:
            f.write(text)
        for name in ("panda-a.log", "panda-a.log-20261008.gz"):
            out = self._grep(pattern="a+b", log_filename=name)
            self.assertEqual(out["result"], "2026-10-08 00:00:01 aaab\n", name)

    def test_fixed_strings(self) -> None:
        # any of the strings matches, each as it is: "." and "+" are no wildcards here
        lines = [
            "2026-10-08 00:00:01 getJobs < SITE 2026-10-08T00:00:01 > start\n",
            "2026-10-08 00:00:02 a.b+c/d\n",
            "2026-10-08 00:00:03 axbbc/d\n",
            "2026-10-08 00:00:04 --pre=true\n",
        ]
        self._write("panda-a.log", "".join(lines))
        out = self._grep(fixed_strings=["getJobs < SITE 2026-10-08T00:00:01 >", "a.b+c/d", "--pre=true"], log_filename="panda-a.log")
        self.assertEqual(out["result"], lines[0] + lines[1] + lines[3])

    def test_matching_lines_alone(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        out = self._grep(pattern="7353014619", log_filename="panda-a.log")
        self.assertEqual(out["result"], RECORDS[0] + RECORDS[1] + RECORDS[7])
        self.assertEqual((out["return_code"], out["truncated"]), (0, False))

    def test_whole_records(self) -> None:
        # each matching line comes with the lines of its record after it, up to the next stamp
        self._write("panda-a.log", "".join(RECORDS))
        with gzip.open(os.path.join(self.logdir, "panda-a.log-20261008.gz"), "wt") as f:
            f.write("".join(RECORDS))
        for name in ("panda-a.log", "panda-a.log-20261008.gz"):
            out = self._grep(pattern="7353014619", log_filename=name, whole_records=True)
            self.assertEqual(json.loads(out["result"]), {"records": [record(0, 1), record(1, 6), record(7, 8)], "window": None}, name)
            self.assertEqual((out["return_code"], out["truncated"]), (0, False), name)

    def test_whole_records_matching_within_a_record(self) -> None:
        # a match in the traceback of a kept record adds no line twice, and a match in the
        # traceback of a record that does not match brings the lines after it, without a head
        self._write("panda-a.log", "".join(RECORDS))
        out = self._grep(pattern="ddm.py|ERROR failed", log_filename="panda-a.log", whole_records=True)
        self.assertEqual(json.loads(out["result"])["records"], [record(1, 6)])
        out = self._grep(pattern="raise X", log_filename="panda-a.log", whole_records=True)
        self.assertEqual(json.loads(out["result"])["records"], [record(4, 6, head=False)])

    def test_whole_records_part_after_a_record(self) -> None:
        # a part of a record that does not match, right after one that does, stays apart from it
        lines = [
            "2026-10-08 00:00:01,001 adder: DEBUG start job=1\n",
            "2026-10-08 00:00:02,001 adder: ERROR failed\n",
            "Traceback (most recent call last):\n",
            "  File x, in f(job=1)\n",
            "ValueError: boom\n",
            "2026-10-08 00:00:03,001 adder: DEBUG other\n",
        ]
        self._write("panda-a.log", "".join(lines))
        out = self._grep(pattern="job=1", log_filename="panda-a.log", whole_records=True)
        self.assertEqual(
            json.loads(out["result"])["records"],
            [record(0, 1, lines), record(3, 5, lines, head=False)],
        )

    def test_whole_records_cut(self) -> None:
        # a record longer than the most lines taken after its match is cut, and says so
        self._write("panda-a.log", "".join(RECORDS))
        with mock.patch.object(processor, "_MAX_CONTINUATION_LINES", 2):
            out = self._grep(pattern="ERROR failed", log_filename="panda-a.log", whole_records=True)
            self.assertEqual(json.loads(out["result"])["records"], [record(1, 4, cut=True)])
            self.assertTrue(out["truncated"])
        # a record of exactly as many lines as are taken is whole
        with mock.patch.object(processor, "_MAX_CONTINUATION_LINES", 4):
            out = self._grep(pattern="ERROR failed", log_filename="panda-a.log", whole_records=True)
            self.assertEqual(json.loads(out["result"])["records"], [record(1, 6)])
            self.assertFalse(out["truncated"])

    def test_whole_records_in_a_tail_window(self) -> None:
        # the window cuts the line of ddm.py, so it starts with the next line; offsets are
        # those in the file, and the window says from which stamp records were searched whole
        self._write("panda-a.log", "".join(RECORDS))
        tail_bytes = len("".join(RECORDS[3:])) - 5
        out = self._grep(pattern="raise X|ddm.py|7353014619", log_filename="panda-a.log", whole_records=True, tail_bytes=tail_bytes)
        self.assertEqual(
            json.loads(out["result"]),
            {"records": [record(4, 6, head=False), record(7, 8)], "window": {"offset": offset(4), "first_stamp": "2026-10-08 00:00:03"}},
        )

    def test_tail_window_starts_with_a_whole_line(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        out = self._grep(pattern="ddm.py|raise X", log_filename="panda-a.log", tail_bytes=len("".join(RECORDS[3:])) - 5)
        self.assertEqual(out["result"], RECORDS[4])
        # a window that starts right after a line break takes the line there
        out = self._grep(pattern="ddm.py|raise X", log_filename="panda-a.log", tail_bytes=len("".join(RECORDS[3:])))
        self.assertEqual(out["result"], RECORDS[3] + RECORDS[4])

    def test_whole_records_result_size_cap(self) -> None:
        # records are kept while they fit, and the result stays valid JSON
        lines = [f"2026-10-08 00:00:01 line {i:06d}\n" for i in range(1000)]
        self._write("panda-a.log", "".join(lines))
        with mock.patch.object(processor, "_MAX_RESULT_BYTES", 3000):
            out = self._grep(pattern="line", log_filename="panda-a.log", whole_records=True)
        self.assertTrue(out["truncated"])
        self.assertLessEqual(len(out["result"]), 3000)
        records = json.loads(out["result"])["records"]
        self.assertEqual(records, [record(i, i + 1, lines) for i in range(len(records))])

    def test_result_size_cap(self) -> None:
        # the matcher is stopped once no more lines fit; the kept lines are whole, and the
        # return code still says that lines matched
        lines = [f"2026-10-08 00:00:01 line {i:06d}\n" for i in range(10000)]
        self._write("panda-a.log", "".join(lines))
        with mock.patch.object(processor, "_MAX_RESULT_BYTES", 1000):
            out = self._grep(pattern="line", log_filename="panda-a.log")
        self.assertTrue(out["truncated"])
        self.assertEqual(out["return_code"], 0)
        self.assertEqual(out["result"], "".join(lines[: 1000 // len(lines[0])]))

    def test_max_matches(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        out = self._grep(pattern="7353014619", log_filename="panda-a.log", max_matches=2)
        self.assertEqual(out["result"], RECORDS[0] + RECORDS[1])
        self.assertTrue(out["truncated"])

    def test_no_match(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        out = self._grep(pattern="nothing", log_filename="panda-a.log")
        self.assertEqual((out["status"], out["result"], out["return_code"], out["truncated"]), ("done", "", 1, False))
        out = self._grep(pattern="nothing", log_filename="panda-a.log", whole_records=True)
        self.assertEqual(json.loads(out["result"]), {"records": [], "window": None})
        self.assertEqual((out["return_code"], out["truncated"]), (1, False))

    def test_timeout(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        with (
            mock.patch.object(processor, "_bounded_matcher", return_value=["sleep", "10"]),
            mock.patch.object(processor, "_SUBPROCESS_TIMEOUT", 0.2),
        ):
            out = self._grep(pattern="x", log_filename="panda-a.log")
        self.assertEqual((out["status"], out["error_msg"]), ("failed", "timeout"))


class TestHandleLogList(unittest.TestCase):
    def setUp(self) -> None:
        self.logdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.logdir)
        patcher = mock.patch.object(panda_config, "logdir", self.logdir, create=True)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _list(self, parameters: dict[str, Any] | None = None) -> dict[str, Any]:
        """Run the handler, and give what it stored."""
        task_buffer = mock.MagicMock()
        processor._handle_log_list({"request_id": "r1", "parameters": json.dumps(parameters or {})}, task_buffer, mock.MagicMock(), "host")
        args, kwargs = task_buffer.finish_async_result.call_args
        return {"status": args[2], **kwargs}

    def test_list(self) -> None:
        # the files a grep may read, rotated or not, and nothing else
        for name, text in (("panda-a.log", "xx"), ("panda-a.log-20261012", "x"), ("panda-a.log-20261008.gz", ""), ("httpd.log", "x")):
            path = os.path.join(self.logdir, name)
            with open(path, "w") as f:
                f.write(text)
            # 2026-10-12 23:59:59 UTC
            os.utime(path, (1791849599, 1791849599))
        os.mkdir(os.path.join(self.logdir, "panda-dir"))
        out = self._list()
        self.assertEqual((out["status"], out["truncated"]), ("done", False))
        self.assertEqual(
            json.loads(out["result"]),
            [
                {"name": "panda-a.log", "size": 2, "mtime": "2026-10-12 23:59:59"},
                {"name": "panda-a.log-20261008.gz", "size": 0, "mtime": "2026-10-12 23:59:59"},
                {"name": "panda-a.log-20261012", "size": 1, "mtime": "2026-10-12 23:59:59"},
            ],
        )

    def test_log_names(self) -> None:
        # the files of the logs named and those rotated from them, and no log whose name only starts the same
        names = (
            "panda-a.log",
            "panda-a.log-20261012",
            "panda-a.log-20261008.gz",
            "panda-ab.log",
            "panda-ab.log-20261012.gz",
            "panda-b.log",
            "panda-b.log-20261012.gz",
        )
        for name in names:
            open(os.path.join(self.logdir, name), "w").close()
        out = self._list({"requester": "alice", "access": "owner", "log_names": ["panda-a.log", "panda-b.log-20261012.gz", "panda-missing.log"]})
        self.assertEqual((out["status"], out["truncated"]), ("done", False))
        self.assertEqual(
            [file["name"] for file in json.loads(out["result"])],
            ["panda-a.log", "panda-a.log-20261008.gz", "panda-a.log-20261012", "panda-b.log-20261012.gz"],
        )

    def test_log_names_cut_short(self) -> None:
        # the list of the logs named is cut as the whole list is
        for i in range(100):
            open(os.path.join(self.logdir, f"panda-a.log-{20260101 + i}.gz"), "w").close()
        open(os.path.join(self.logdir, "panda-0.log"), "w").close()
        with mock.patch.object(processor, "_MAX_RESULT_BYTES", 1000):
            out = self._list({"log_names": ["panda-a.log"]})
        self.assertTrue(out["truncated"])
        files = json.loads(out["result"])
        self.assertEqual([file["name"] for file in files], [f"panda-a.log-{20260101 + i}.gz" for i in range(len(files))])

    def test_list_cut_short(self) -> None:
        # files are listed while they fit, and the list stays valid JSON
        for i in range(100):
            open(os.path.join(self.logdir, f"panda-{i:03d}.log"), "w").close()
        with mock.patch.object(processor, "_MAX_RESULT_BYTES", 1000):
            out = self._list()
        self.assertTrue(out["truncated"])
        files = json.loads(out["result"])
        self.assertLessEqual(len(out["result"]), 1000)
        self.assertEqual([file["name"] for file in files], [f"panda-{i:03d}.log" for i in range(len(files))])

    def test_no_logdir(self) -> None:
        with mock.patch.object(panda_config, "logdir", os.path.join(self.logdir, "missing")):
            out = self._list()
        self.assertEqual(out["status"], "failed")


class TestGrepInCycle(unittest.TestCase):
    """A grep starts only when the cycle has time left for its longest run."""

    DAEMON = {"module": "async_request_daemon", "period": 10, "timeout": 300}

    def test_cycle_seconds(self) -> None:
        with mock.patch.object(daemon_config, "find_daemon", return_value=self.DAEMON):
            # the timeout less the seconds a result is stored in
            self.assertEqual(processor._cycle_seconds(SERVICE_SERVER), 270)
            # nothing kills a cycle of JEDI's watchdog
            self.assertIsNone(processor._cycle_seconds(SERVICE_JEDI))
        with mock.patch.object(daemon_config, "find_daemon", return_value=None):
            self.assertIsNone(processor._cycle_seconds(SERVICE_SERVER))

    def test_fits_in_cycle(self) -> None:
        now = [1000.0]
        with mock.patch.object(processor, "time", SimpleNamespace(monotonic=lambda: now[0])):
            # 240 s from 30 s into a cycle of 270 s ends with it, a second later does not
            now[0] = 1030.0
            self.assertTrue(processor._fits_in_cycle("grep", 270, 1000.0))
            now[0] = 1031.0
            self.assertFalse(processor._fits_in_cycle("grep", 270, 1000.0))
            # a type that does not run long, a cycle nothing kills, and a cycle too short
            # for a grep at all start it as before
            self.assertTrue(processor._fits_in_cycle("log_list", 270, 1000.0))
            self.assertTrue(processor._fits_in_cycle("grep", None, 1000.0))
            self.assertTrue(processor._fits_in_cycle("grep", 30, 1000.0))

    def test_run_leaves_a_grep_for_the_next_cycle(self) -> None:
        # the first grep takes 100 s, so the second would start 100 s into a cycle of 270 s
        now = [1000.0]
        ran: list[str] = []

        def grep(row: dict[str, Any], tb: Any, tmp_logger: Any, result_machine: str) -> None:
            ran.append(row["request_id"])
            now[0] += 100

        task_buffer = mock.MagicMock()
        task_buffer.get_pending_requests_for_machine.return_value = [
            {"request_id": request_id, "request_type": "grep", "machine_name": None, "parameters": "{}"} for request_id in ("r1", "r2")
        ]
        task_buffer.claim_async_result.return_value = True
        with (
            mock.patch.object(processor, "time", SimpleNamespace(monotonic=lambda: now[0])),
            mock.patch.dict(processor.HANDLERS, {"grep": grep}),
            mock.patch.object(daemon_config, "find_daemon", return_value=self.DAEMON),
            # run sets the start of the cycle, on the fake clock, where db_read reads it
            mock.patch.object(processor, "_cycle_started_at", None),
        ):
            processor.run(SERVICE_SERVER, task_buffer)
        self.assertEqual(ran, ["r1"])
        # not claimed, so it stays pending
        self.assertEqual([c.args[0] for c in task_buffer.claim_async_result.call_args_list], ["r1"])

    def test_run_takes_a_grep_after_the_other_requests(self) -> None:
        # a db_read submitted after a grep still runs first, with the whole cycle before it,
        # rather than after the grep with what the grep left
        now = [1000.0]
        ran: list[tuple[str, float]] = []

        def handle(row: dict[str, Any], tb: Any, tmp_logger: Any, result_machine: str) -> None:
            ran.append((row["request_id"], now[0]))
            now[0] += 100

        task_buffer = mock.MagicMock()
        task_buffer.get_pending_requests_for_machine.return_value = [
            {"request_id": "r1", "request_type": "grep", "machine_name": None, "parameters": "{}"},
            {"request_id": "r2", "request_type": "db_read", "machine_name": "any", "parameters": "{}"},
            {"request_id": "r3", "request_type": "log_list", "machine_name": None, "parameters": "{}"},
        ]
        task_buffer.claim_async_result.return_value = True
        with (
            mock.patch.object(processor, "time", SimpleNamespace(monotonic=lambda: now[0])),
            mock.patch.dict(processor.HANDLERS, {"grep": handle, "db_read": handle, "log_list": handle}),
            mock.patch.object(daemon_config, "find_daemon", return_value=self.DAEMON),
            mock.patch.object(processor, "_cycle_started_at", None),
        ):
            processor.run(SERVICE_SERVER, task_buffer)
        # the others in the order they came, and then no grep, since 200 s of the cycle are gone
        self.assertEqual(ran, [("r2", 1000.0), ("r3", 1100.0)])


if __name__ == "__main__":
    unittest.main()
