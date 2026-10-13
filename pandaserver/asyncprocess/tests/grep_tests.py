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
            self.assertEqual(out["result"], "".join(RECORDS[0:6]) + RECORDS[7], name)
            self.assertEqual((out["return_code"], out["truncated"]), (0, False), name)

    def test_whole_records_matching_within_a_record(self) -> None:
        # a match in the traceback of a kept record adds no line twice, and a match in the
        # traceback of a record that does not match brings the lines after it
        self._write("panda-a.log", "".join(RECORDS))
        out = self._grep(pattern="ddm.py|ERROR failed", log_filename="panda-a.log", whole_records=True)
        self.assertEqual(out["result"], "".join(RECORDS[1:6]))
        out = self._grep(pattern="raise X", log_filename="panda-a.log", whole_records=True)
        self.assertEqual(out["result"], "".join(RECORDS[4:6]))

    def test_whole_records_cut(self) -> None:
        # a record longer than the most lines taken after its match is cut, and says so
        self._write("panda-a.log", "".join(RECORDS))
        with mock.patch.object(processor, "_MAX_CONTINUATION_LINES", 2):
            out = self._grep(pattern="ERROR failed", log_filename="panda-a.log", whole_records=True)
            self.assertEqual(out["result"], "".join(RECORDS[1:4]))
            self.assertTrue(out["truncated"])
        # a record of exactly as many lines as are taken is whole
        with mock.patch.object(processor, "_MAX_CONTINUATION_LINES", 4):
            out = self._grep(pattern="ERROR failed", log_filename="panda-a.log", whole_records=True)
            self.assertEqual(out["result"], "".join(RECORDS[1:6]))
            self.assertFalse(out["truncated"])

    def test_whole_records_in_a_tail_window(self) -> None:
        self._write("panda-a.log", "".join(RECORDS))
        tail_bytes = len("".join(RECORDS[4:]))
        out = self._grep(pattern="raise X|7353014619", log_filename="panda-a.log", whole_records=True, tail_bytes=tail_bytes)
        self.assertEqual(out["result"], RECORDS[4] + RECORDS[5] + RECORDS[7])

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
        out = self._grep(pattern="nothing", log_filename="panda-a.log", whole_records=True)
        self.assertEqual((out["status"], out["result"], out["return_code"], out["truncated"]), ("done", "", 1, False))

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

    def _list(self) -> dict[str, Any]:
        """Run the handler, and give what it stored."""
        task_buffer = mock.MagicMock()
        processor._handle_log_list({"request_id": "r1", "parameters": "{}"}, task_buffer, mock.MagicMock(), "host")
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


if __name__ == "__main__":
    unittest.main()
