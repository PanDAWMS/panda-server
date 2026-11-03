"""
Shared async request processing logic.
Each service wraps this with a thin entrypoint that passes its service_name.
New request types: add a handler function and register it in HANDLERS.
"""

import contextlib
import datetime
import json
import os
import re
import socket
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable
from typing import Any, BinaryIO

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.asyncprocess import TaskBufferLike, data_carousel_handlers, db_read_handlers
from pandaserver.config import daemon_config, panda_config
from pandaserver.taskbuffer.db_proxy_mods.async_request_module import ANY_MACHINE, SERVICE_SERVER

_logger = PandaLogger().getLogger("async_request_processor")

MY_HOSTNAME = socket.getfqdn()

# max subprocess timeout in seconds
_SUBPROCESS_TIMEOUT = 240

# stale result threshold: must exceed the longest handler subprocess timeout
_STALE_THRESHOLD_SECONDS = _SUBPROCESS_TIMEOUT * 2

# max result size stored in DB (bytes)
_MAX_RESULT_BYTES = 1_000_000

# Longest a request of each type may run, for the types whose requests can
# run long.  One is not started when the cycle has less time left than that,
# and stays pending for the next cycle: started anyway, it would be killed with
# the cycle, wait out _STALE_THRESHOLD_SECONDS, and fail after three attempts.
# They also come after the other requests of a cycle, which a long one before
# them would leave too little time: a db_read is refused when the cycle has
# none left for its queries.
_LONGEST_RUN_SECONDS = {"grep": _SUBPROCESS_TIMEOUT}

# time.monotonic() when the cycle running in this process started. The daemon's timeout counts
# from then, over every request the cycle processes, so a handler with a deadline of its own
# reads it through get_cycle_started_at()
_cycle_started_at: float | None = None


def _cycle_seconds(service_name: str) -> int | None:
    """
    Get how long a cycle may take requests for, before the daemon master kills it.

    Only async_request_daemon of the server runs under such a timeout.  JEDI's
    AsyncRequestWatchDog has none, and a cycle run by hand with no enabled
    entry in the daemon config has none either.  The seconds that a db_read
    keeps at the end to store its result are kept here too.

    Args:
        service_name(str): the service the cycle runs for

    Returns:
        int or None: the seconds, or None when nothing kills the cycle
    """
    if service_name != SERVICE_SERVER:
        return None
    daemon = daemon_config.find_daemon(db_read_handlers.DAEMON_MODULE)
    if daemon is None:
        return None
    return daemon_config.get_timeout(daemon) - db_read_handlers.MARGIN_SECONDS


def _fits_in_cycle(request_type: str, cycle_seconds: int | None, cycle_started_at: float) -> bool:
    """
    Tell whether a request of this type can run to its end before the cycle is killed.

    A cycle too short for the longest run of a type at all still starts its
    requests, as it always did, since leaving them pending would leave them
    there for good.

    Args:
        request_type(str): the type of the request
        cycle_seconds(int or None): what _cycle_seconds gives
        cycle_started_at(float): time.monotonic() when the cycle started

    Returns:
        bool: False when the request should wait for the next cycle
    """
    longest = _LONGEST_RUN_SECONDS.get(request_type)
    if longest is None or cycle_seconds is None or longest > cycle_seconds:
        return True
    return time.monotonic() + longest <= cycle_started_at + cycle_seconds


def get_cycle_started_at() -> float | None:
    """
    Get when the processing cycle running in this process started.

    Returns:
        float or None: time.monotonic() at the start of the cycle, or None outside one
    """
    return _cycle_started_at


# Cap on how many matching lines a grep may produce.  Log files here reach
# several gigabytes, and an unselective pattern against one of those makes the
# matcher stream the whole file back.  The output is read as it comes and the
# matcher stopped once _MAX_RESULT_BYTES of it are kept, so the daemon holds no
# more than that; the cap stops the matcher sooner when a caller asks for fewer
# lines.  The default is set high enough that the stored result and its
# truncated flag are what they were before.
_DEFAULT_MAX_MATCHES = 20000
_MAX_MAX_MATCHES = 200000

# A log record starts with its time stamp.  The lines of a traceback, or of
# any other message of several lines, continue it without one.
_RECORD_START = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
_RECORD_START_BYTES = re.compile(rb"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

# Most lines a grep for whole records takes after a matching line.  A record
# cut there makes the result come back truncated, as reaching any other cap
# does, so that the cut record is not read as whole.
_MAX_CONTINUATION_LINES = 200

# A line of the matcher's output with -b: the byte offset at which the line
# starts, then ":" when the line matches or "-" when it is one of the lines
# after a match (-A).  Between lines that are not adjacent comes a line of "--".
_OFFSET_LINE = re.compile(r"(\d+)([:-])")

# Room that a record takes in the JSON of a result besides its lines, and room
# for the rest of that JSON; both more than they ever take.
_RECORD_JSON_BYTES = 100
_RESULT_JSON_BYTES = 1000

# Most bytes read from a point of a file to find the first time stamp after it,
# for where a window starts and ends.  Bisecting a file on its time stamps stops
# at a range of this size too.
_WINDOW_STAMP_SCAN_BYTES = 1_000_000

# A time stamp as a log writes it, to the second, which compares as a string.
_STAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# How far a time window reaches beyond the times asked, for records written out
# of the order of their stamps.  A record is stamped when it is made, and
# written after by one of the processes writing the file, so its line can come
# after one stamped later: by at most 75 ms in what was measured (2026-11), and
# by seconds when writes stall.  This is only about the order within the file.
# The clock the times asked come from may differ from this machine's, which is
# for the caller to widen its window by.
_WINDOW_SLACK = datetime.timedelta(seconds=60)

# Most bytes of a time window that are searched.  Cold disks read 7 to 33 MB/s
# (2026-10 and 11), so these take about 150 s at the slowest, within the 240 s a
# grep may run.  A wider window is cut and says so, and the caller asks again
# from where it stopped, so a bound on the low side costs another request at most.
_MAX_WINDOW_BYTES = 1 << 30

# Bytes of a window read and passed to the matcher at a time.
_READ_CHUNK_BYTES = 8 << 20

# The name of the thread that passes a window to the matcher.
_READER_THREAD_NAME = "grep-window-reader"

# A file opened at an arbitrary offset is not a valid gzip stream, so a
# window is only meaningful on plain text.
_GZIP_SUFFIX = ".gz"

# Kept verbatim because callers distinguish "the file is not there" -- which
# says the code writing it never ran -- from a query that failed for some other
# reason, and that distinction must not depend on which tool reported it.
_NO_SUCH_FILE = "No such file or directory"

# logrotate writes a .gz under its final name and removes the file it compresses
# only once it is done, so a .gz whose file is still there is incomplete: a grep
# would miss what is not written yet, and its time of last change is now.  Kept
# verbatim as callers ask again later on it.
_BEING_COMPRESSED = "Being compressed"


def _compressed_from(file_name: str) -> str | None:
    """Give the name of the file a .gz is compressed from, or None for a file of another kind."""
    return file_name.removesuffix(_GZIP_SUFFIX) if file_name.endswith(_GZIP_SUFFIX) else None


def _bounded_matcher(log_path: str, patterns: list[str], fixed: bool, max_matches: int, whole_records: bool, from_stdin: bool) -> list[str]:
    """The matcher command, capped, reading a path or standard input.

    A line matches when any of the patterns does, read as plain text when fixed
    is set and as regular expressions otherwise. Each pattern goes after -e,
    never in a place of its own: there a pattern starting with "-" is read as an
    option, and rg's --pre=COMMAND runs COMMAND on every file under the daemon's
    working directory.

    rg reads a .gz file too, decompressing it with -z, so that a pattern means
    the same in every file. zgrep, used before, read it as a basic regular
    expression of grep, where "+" and "|" are plain characters.

    For whole records, each match comes with one line more than a record may
    continue over, each line with its byte offset, so that _GrepOutput can tell
    where a record starts and ends, and whether it was cut.
    """
    cmd = ["rg", "-m", str(max_matches)]
    if fixed:
        cmd.append("-F")
    if whole_records:
        cmd += ["-b", "-A", str(_MAX_CONTINUATION_LINES + 1)]
    for pattern in patterns:
        cmd += ["-e", pattern]
    if log_path.endswith(_GZIP_SUFFIX):
        cmd.append("-z")
    return cmd if from_stdin else cmd + [log_path]


class _GrepOutput:
    """What a grep keeps of the matcher's output, up to _MAX_RESULT_BYTES.

    The matching lines alone are kept as they come.  Whole records are kept as
    {"offset": int, "head": bool, "lines": [str, ...], "cut": bool}, for the
    JSON of the result: offset is where the first line given starts in the
    file, and head tells whether that line starts the record with its time
    stamp, or matched within the lines of a record that did not match.
    """

    def __init__(self, whole_records: bool, base_offset: int = 0) -> None:
        self.whole_records = whole_records
        # where in the file the matcher's input starts, which a window moves
        self.base_offset = base_offset
        self.lines: list[str] = []
        self.records: list[dict[str, Any]] = []
        # for a window, the bytes searched, as _time_window gives them
        self.window: dict[str, Any] | None = None
        self.size = _RESULT_JSON_BYTES if whole_records else 0
        self.matches = 0
        # whether _MAX_RESULT_BYTES are kept, which stops the matcher
        self.full = False
        # whether a record was cut at _MAX_CONTINUATION_LINES
        self.cut = False
        # whether the lines after a match still continue the record it is in,
        # and how many of them have been kept
        self._in_record = False
        self._continuation = 0

    def add(self, line: str) -> bool:
        """Take one line of the matcher's output, and say whether more fit."""
        if not self.whole_records:
            self.matches += 1
            if not self._fits(len(line)):
                return False
            self.lines.append(line)
            return True

        located = _OFFSET_LINE.match(line)
        if located is None:
            # the "--" between lines that are not adjacent
            self._in_record = False
            return True
        text = line[located.end() :].removesuffix("\n")
        starts_record = _RECORD_START.match(text) is not None
        if located.group(2) == ":":
            self.matches += 1
            # a match without a stamp in a kept record only continues it
            if self._in_record and not starts_record:
                self._continuation += 1
                return self._add_to_record(text)
            if not self._fits(_RECORD_JSON_BYTES + len(json.dumps(text))):
                return False
            self.records.append({"offset": self.base_offset + int(located.group(1)), "head": starts_record, "lines": [text], "cut": False})
            self._in_record = True
            self._continuation = 0
            return True
        if not self._in_record or starts_record:
            self._in_record = False
            return True
        if self._continuation >= _MAX_CONTINUATION_LINES:
            self._in_record = False
            self.records[-1]["cut"] = True
            self.cut = True
            return True
        self._continuation += 1
        return self._add_to_record(text)

    def result(self) -> str:
        """The result to store: the lines as they came, or the records as JSON."""
        if not self.whole_records:
            return "".join(self.lines)
        return json.dumps({"records": self.records, "window": self.window})

    def _add_to_record(self, text: str) -> bool:
        if not self._fits(len(json.dumps(text)) + 2):
            return False
        self.records[-1]["lines"].append(text)
        return True

    def _fits(self, size: int) -> bool:
        if self.size + size > _MAX_RESULT_BYTES:
            self.full = True
            return False
        self.size += size
        return True


def _record_start_at(f: BinaryIO, offset: int) -> tuple[int, str] | None:
    """
    Find the first record of a file that starts at or after an offset.

    A line that the offset cuts is not taken, nor is a line read only in part,
    so that a time stamp is only ever read at the start of a line.

    Args:
        f(BinaryIO): the file
        offset(int): where to start looking

    Returns:
        where the record starts and its time stamp, or None when no line that
        starts within _WINDOW_STAMP_SCAN_BYTES of the offset starts with one
    """
    at_line_start = True
    if offset > 0:
        f.seek(offset - 1)
        at_line_start = f.read(1) == b"\n"
    else:
        f.seek(0)
    scanned = 0
    while scanned < _WINDOW_STAMP_SCAN_BYTES:
        line = f.readline(_WINDOW_STAMP_SCAN_BYTES - scanned)
        if not line:
            break
        stamp = _RECORD_START_BYTES.match(line) if at_line_start else None
        if stamp is not None:
            return offset + scanned, stamp.group().decode()
        scanned += len(line)
        at_line_start = line.endswith(b"\n")
    return None


def _tail_window(f: BinaryIO, size: int, tail_bytes: int) -> dict[str, Any]:
    """
    Find where the tail window of a file starts, and the first time stamp in it.

    The window starts with the first line that starts within the last
    tail_bytes of the file, so that it holds no line in part, and runs to the
    end of the file.

    Args:
        f(BinaryIO): the file
        size(int): its size
        tail_bytes(int): how many bytes from the end the window takes at most

    Returns:
        the window, as _time_window gives it, with no end
    """
    start = max(0, size - tail_bytes)
    if start > 0:
        f.seek(start - 1)
        # unless a line ends right before the window, skip the rest of the line it cuts
        if f.read(1) != b"\n":
            f.readline()
        start = f.tell()
    first = _record_start_at(f, start)
    return {"offset": start, "first_stamp": None if first is None else first[1], "end": None, "end_stamp": None, "cut": False}


def _bisect_stamps(f: BinaryIO, size: int, target: str, unstamped_before: bool) -> tuple[tuple[int, str] | None, tuple[int, str] | None]:
    """
    Bisect a file on the time stamps of its records, around a time.

    The bytes between the two records found hold every record stamped across
    the time, but for records out of order, and are not much more than
    _WINDOW_STAMP_SCAN_BYTES.  A point from which no record is found counts
    as before the time or after it as asked, so that the caller can have the
    bytes widen rather than narrow where there are no stamps to go by.

    Args:
        f(BinaryIO): the file
        size(int): its size
        target(str): the time, as _STAMP_FORMAT writes it
        unstamped_before(bool): whether a point without a record counts as before the time

    Returns:
        the last record found that is stamped before the time, or None for the
        start of the file, and the first found that is stamped at or after it,
        or None for the end of the file
    """
    low, high = 0, size
    before: tuple[int, str] | None = None
    after: tuple[int, str] | None = None
    while high - low > _WINDOW_STAMP_SCAN_BYTES:
        middle = (low + high) // 2
        record = _record_start_at(f, middle)
        if unstamped_before if record is None else record[1] < target:
            low, before = middle, record
        else:
            high, after = middle, record
    return before, after


def _time_window(f: BinaryIO, size: int, since: datetime.datetime | None, until: datetime.datetime | None) -> dict[str, Any]:
    """
    Find the bytes of a plain file that hold its records of a time window.

    The window starts at a record stamped before since less _WINDOW_SLACK, or
    at the start of the file, and ends at a record stamped at or after until
    plus _WINDOW_SLACK, or at the end of the file.  So it holds every record
    stamped at or after since and before until, as long as none is further
    out of order than that.  Where there are no stamps to go by, it widens
    rather than narrows.  A window of more bytes than _MAX_WINDOW_BYTES is cut
    at the first record after that many.

    Args:
        f(BinaryIO): the file
        size(int): its size
        since(datetime or None): the start of the time window, in UTC without tzinfo, or None for the start of the file
        until(datetime or None): its end, likewise, or None for the end of the file

    Returns:
        {"offset": int, "first_stamp": str or None, "end": int or None, "end_stamp": str or None, "cut": bool}:
        where the window starts, and the time stamp of the first line from there
        that starts with one; where it ends, a record not in it, and that
        record's time stamp, both None when it runs to the end of the file as it
        is when read; and whether it was cut
    """
    first: tuple[int, str] | None = None
    if since is not None:
        first, _ = _bisect_stamps(f, size, (since - _WINDOW_SLACK).strftime(_STAMP_FORMAT), unstamped_before=False)
    start = 0 if first is None else first[0]
    if first is None:
        first = _record_start_at(f, 0)
    end: tuple[int, str] | None = None
    if until is not None:
        _, end = _bisect_stamps(f, size, (until + _WINDOW_SLACK).strftime(_STAMP_FORMAT), unstamped_before=True)
    window: dict[str, Any] = {"offset": start, "first_stamp": None if first is None else first[1], "end": None, "end_stamp": None, "cut": False}
    if end is not None:
        # only records far out of order put the end before the start, and then nothing is in between
        window["end"], window["end_stamp"] = (end[0], end[1]) if end[0] >= start else (start, window["first_stamp"])
    limit = size if window["end"] is None else window["end"]
    if limit - start > _MAX_WINDOW_BYTES:
        after_cut = _record_start_at(f, start + _MAX_WINDOW_BYTES)
        if after_cut is None:
            window.update(end=start + _MAX_WINDOW_BYTES, end_stamp=None, cut=True)
        elif after_cut[0] < limit:
            window.update(end=after_cut[0], end_stamp=after_cut[1], cut=True)
    return window


def _feed(f: BinaryIO, start: int, end: int | None, sink: int, errors: list[Exception]) -> None:
    """
    Write a window of a file to the matcher's standard input, and close it.

    Run in a thread of its own.  A matcher stopped before it has read the whole
    window, at a cap or at the timeout, ends this at its next write.  Any other
    failure is kept in errors, so that a window read only in part is not taken
    for one searched whole.

    Args:
        f(BinaryIO): the file
        start(int): where the window starts
        end(int or None): where it ends, or None for the end of the file as it is when reached
        sink(int): the write end of the pipe to the matcher, which this closes
        errors(list): where a failure is kept
    """
    try:
        with open(sink, "wb") as out:
            f.seek(start)
            position = start
            while end is None or position < end:
                chunk = f.read(_READ_CHUNK_BYTES if end is None else min(_READ_CHUNK_BYTES, end - position))
                if not chunk:
                    break
                out.write(chunk)
                position += len(chunk)
    except BrokenPipeError:
        pass
    except Exception as e:
        errors.append(e)


def _run_grep(
    log_path: str,
    patterns: list[str],
    fixed: bool,
    max_matches: int,
    tail_bytes: int | None,
    whole_records: bool,
    since: datetime.datetime | None = None,
    until: datetime.datetime | None = None,
) -> tuple[_GrepOutput, int, str]:
    """Run the matcher, optionally over just a window of the file.

    The pipe is built process to process rather than through a shell: the
    pattern is caller-supplied, so handing it to a shell would hand the caller
    the daemon.

    A window of a plain file, its tail or the bytes of a time window, is read
    by a thread that writes it to the matcher's standard input.  The thread
    reads the file as it was opened to find the window, so a rotation in
    between cannot make it read another file.

    The output is read as it comes, and the matcher stopped once it no longer
    fits, so the daemon never holds more of it than what is kept.  A matcher so
    stopped had matched, and its return code is given as 0.

    Returns:
        the output kept, the return code of the matcher, and its standard error
    """
    windowed = bool(tail_bytes or since or until) and not log_path.endswith(_GZIP_SUFFIX)
    cmd = _bounded_matcher(log_path, patterns, fixed, max_matches, whole_records, from_stdin=windowed)
    output = _GrepOutput(whole_records)
    read_errors: list[Exception] = []
    with contextlib.ExitStack() as stack:
        # standard error goes to a file, since a pipe nobody reads until the end
        # could fill and stall the matcher
        stderr_file = stack.enter_context(tempfile.TemporaryFile())
        matcher_input = subprocess.DEVNULL
        if windowed:
            log_file = stack.enter_context(open(log_path, "rb"))
            size = os.fstat(log_file.fileno()).st_size
            output.window = _tail_window(log_file, size, tail_bytes) if tail_bytes else _time_window(log_file, size, since, until)
            output.base_offset = output.window["offset"]
            matcher_input, sink = os.pipe()
            reader = threading.Thread(
                target=_feed, args=(log_file, output.base_offset, output.window["end"], sink, read_errors), name=_READER_THREAD_NAME, daemon=True
            )
            reader.start()
            # joined before the file is closed, which is once the matcher is gone
            # and the thread has been told so at its next write
            stack.callback(reader.join)
        try:
            matcher = subprocess.Popen(
                cmd,
                stdin=matcher_input,
                stdout=subprocess.PIPE,
                stderr=stderr_file,
                text=True,
                errors="replace",
            )
        finally:
            # Hand the read end over completely.  Keeping a copy here would leave the
            # matcher's exit at the cap invisible to the thread, which would then block
            # on a full pipe instead of being told to stop.
            if windowed:
                os.close(matcher_input)

        timed_out = threading.Event()

        def stop_at_timeout() -> None:
            timed_out.set()
            matcher.kill()

        timer = threading.Timer(_SUBPROCESS_TIMEOUT, stop_at_timeout)
        timer.start()
        try:
            if matcher.stdout is not None:
                for line in matcher.stdout:
                    if not output.add(line):
                        break
        finally:
            timer.cancel()
            if matcher.poll() is None:
                matcher.kill()
            matcher.wait()
            if matcher.stdout is not None:
                matcher.stdout.close()
        if timed_out.is_set():
            raise subprocess.TimeoutExpired(cmd, _SUBPROCESS_TIMEOUT)
        stderr_file.seek(0)
        stderr = stderr_file.read(_MAX_RESULT_BYTES + 1).decode(errors="replace")
    if read_errors:
        raise read_errors[0]
    return_code = 0 if output.full else matcher.returncode
    return output, return_code, stderr


def _handle_grep(row: dict[str, Any], tb: TaskBufferLike, tmp_logger: LogWrapper, result_machine: str) -> None:
    """Run rg on a log file and store the output under result_machine's result row."""
    params = json.loads(row["parameters"])
    log_filename = params["log_filename"]
    # either one regular expression or any number of plain strings
    fixed_strings = params.get("fixed_strings")
    fixed = fixed_strings is not None
    patterns = fixed_strings if fixed else [params["pattern"]]
    max_matches = min(int(params.get("max_matches") or _DEFAULT_MAX_MATCHES), _MAX_MAX_MATCHES)
    tail_bytes = params.get("tail_bytes")
    whole_records = bool(params.get("whole_records"))
    # in UTC without an offset, as the time stamps of the logs are
    since = datetime.datetime.fromisoformat(params["since"]) if params.get("since") else None
    until = datetime.datetime.fromisoformat(params["until"]) if params.get("until") else None
    log_path = os.path.join(panda_config.logdir, log_filename)

    # Checked here rather than left to the tool, because with a window the
    # matcher reads standard input and never sees the path -- it would report
    # "no match" for a file that does not exist.  One answer, whichever way the
    # query was built.
    if not os.path.isfile(log_path):
        tmp_logger.debug(f"{log_path}: {_NO_SUCH_FILE}")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg=f"{log_path}: {_NO_SUCH_FILE}",
            retriable=False,
        )
        return

    # Checked once, as the file compressed from is removed only after the .gz
    # is complete.  Not retried by the processor either, since a compression
    # takes longer than its attempts: the caller asks again later.
    compressed_from = _compressed_from(log_filename)
    if compressed_from is not None and os.path.isfile(os.path.join(panda_config.logdir, compressed_from)):
        tmp_logger.debug(f"{log_path}: {_BEING_COMPRESSED} from {compressed_from}")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg=f"{log_path}: {_BEING_COMPRESSED}",
            retriable=False,
        )
        return

    shown = f"{len(patterns)} fixed strings {patterns[:3]!r}" if fixed else repr(patterns[0])
    tmp_logger.debug(
        f"grep {shown} in {log_path} (max_matches={max_matches}, tail_bytes={tail_bytes}, since={since}, until={until}, whole_records={whole_records})"
    )
    try:
        start_time = naive_utcnow()
        output, return_code, stderr = _run_grep(log_path, patterns, fixed, max_matches, tail_bytes, whole_records, since, until)
        elapsed = (naive_utcnow() - start_time).total_seconds()
        tmp_logger.debug(f"subprocess completed in {elapsed:.2f} seconds")
    except subprocess.TimeoutExpired:
        tmp_logger.error(f"subprocess timed out after {_SUBPROCESS_TIMEOUT} seconds")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg="timeout",
            retriable=False,
        )
        return
    except Exception as e:
        tmp_logger.error(f"subprocess failed with exception: {e}")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg=str(e),
            retriable=False,
        )
        return

    # Reaching the cap truncates just as surely as exceeding the byte limit
    # does, and a caller that reads an incomplete result as "this never appears
    # in the log" gets the wrong answer, so both set the same flag.  So does a
    # record cut short.  A time window cut short does not: it says so itself,
    # since it calls for asking further rather than differently, and a caller
    # must be able to tell which happened, or whether both did.
    capped = output.matches >= max_matches
    truncated = capped or output.full or output.cut or len(stderr) > _MAX_RESULT_BYTES
    window = output.window
    shown_window = "" if window is None else f", window {window['offset']} to {window['end']} cut={window['cut']}"
    tmp_logger.debug(
        f"outcome: return code {return_code}, {output.matches} matches, result size {output.size}, stderr size {len(stderr)}, "
        f"truncated={truncated} (capped={capped} full={output.full} cut={output.cut}){shown_window}"
    )
    tb.finish_async_result(
        row["request_id"],
        result_machine,
        "done",
        result=output.result(),
        stderr=stderr[:_MAX_RESULT_BYTES],
        return_code=return_code,
        truncated=truncated,
    )


def _is_named(file_name: str, log_names: set[str]) -> bool:
    """Tell whether a file is one of the logs named or rotated from one: the name followed by "-"."""
    return file_name in log_names or any(file_name[:i] in log_names for i, c in enumerate(file_name) if c == "-")


def _handle_log_list(row: dict[str, Any], tb: TaskBufferLike, tmp_logger: LogWrapper, result_machine: str) -> None:
    """List the log files under panda_config.logdir as JSON, and store it under result_machine's result row.

    Every regular file whose name starts with "panda-" is listed, as a grep may
    read no other, with its size and its time of last change in UTC, except a
    .gz still being compressed.  With log_names, only the files of those logs
    are.  Files are listed in the order of their names until the list no longer
    fits in _MAX_RESULT_BYTES, which sets truncated.
    """
    # the requests submitted before log_names was taken have none
    log_names = json.loads(row["parameters"]).get("log_names")
    named = None if log_names is None else set(log_names)
    try:
        with os.scandir(panda_config.logdir) as entries:
            files: list[dict[str, Any]] = []
            # every file, named or not, as a .gz named alone is compressed from a file not named
            present: set[str] = set()
            for entry in entries:
                if not entry.name.startswith("panda-") or not entry.is_file():
                    continue
                present.add(entry.name)
                # a file being compressed is listed for its .gz too, which is left out
                if named is not None and not (_is_named(entry.name, named) or entry.name + _GZIP_SUFFIX in named):
                    continue
                stat = entry.stat()
                mtime = datetime.datetime.fromtimestamp(stat.st_mtime, datetime.timezone.utc)
                files.append({"name": entry.name, "size": stat.st_size, "mtime": mtime.strftime("%Y-%m-%d %H:%M:%S")})
    except OSError as e:
        tmp_logger.error(f"failed to list {panda_config.logdir}: {e}")
        tb.finish_async_result(row["request_id"], result_machine, "failed", error_msg=str(e), retriable=False)
        return

    # the file being compressed is listed, so that a caller knows its .gz is to come
    files = [file for file in files if _compressed_from(file["name"]) not in present]
    files.sort(key=lambda file: file["name"])
    # one entry at a time, so that a list cut short is still valid JSON
    kept: list[str] = []
    size = 2
    for file in files:
        text = json.dumps(file)
        if size + len(text) + 2 > _MAX_RESULT_BYTES:
            break
        kept.append(text)
        size += len(text) + 2
    truncated = len(kept) < len(files)
    shown = "" if log_names is None else f" of {len(log_names)} logs {log_names[:3]!r}"
    tmp_logger.debug(f"listed {len(kept)} of {len(files)} files{shown} in {panda_config.logdir}")
    tb.finish_async_result(
        row["request_id"],
        result_machine,
        "done",
        result=f"[{', '.join(kept)}]",
        truncated=truncated,
    )


def _handle_sleep_echo(row: dict[str, Any], tb: TaskBufferLike, tmp_logger: LogWrapper, result_machine: str) -> None:
    """Sleep for the requested seconds, then echo the message; store echo stdout as the result."""
    params = json.loads(row["parameters"])
    seconds = params["seconds"]
    message = params["message"]

    tmp_logger.debug(f"sleeping {seconds}s then echoing")
    try:
        subprocess.run(["sleep", str(seconds)], capture_output=True, text=True, timeout=_SUBPROCESS_TIMEOUT)
        proc = subprocess.run(["echo", message], capture_output=True, text=True, timeout=_SUBPROCESS_TIMEOUT)
    except subprocess.TimeoutExpired:
        tmp_logger.error(f"subprocess timed out after {_SUBPROCESS_TIMEOUT} seconds")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg="timeout",
            retriable=False,
        )
        return
    except Exception as e:
        tmp_logger.error(f"subprocess failed with exception: {e}")
        tb.finish_async_result(
            row["request_id"],
            result_machine,
            "failed",
            error_msg=str(e),
            retriable=False,
        )
        return

    stdout = proc.stdout
    stderr = proc.stderr
    truncated = len(stdout) > _MAX_RESULT_BYTES or len(stderr) > _MAX_RESULT_BYTES
    tmp_logger.debug(f"outcome: return code {proc.returncode}, stdout size {len(stdout)}, truncated={truncated}")
    tb.finish_async_result(
        row["request_id"],
        result_machine,
        "done",
        result=stdout[:_MAX_RESULT_BYTES],
        stderr=stderr[:_MAX_RESULT_BYTES],
        return_code=proc.returncode,
        truncated=truncated,
    )


# Register new request types here — no new daemon needed. The handlers take the same four
# arguments but are otherwise unrelated functions, hence the bare Callable
HANDLERS: dict[str, Callable[..., None]] = {
    "grep": _handle_grep,
    "log_list": _handle_log_list,
    "sleep_echo": _handle_sleep_echo,
    # Data Carousel operations, submitted by pandaserver.api.v1.data_carousel_api
    **data_carousel_handlers.HANDLERS,
    # reads of the database, submitted by pandaserver.api.v1.async_process_api
    **db_read_handlers.HANDLERS,
}


def run(service_name: str, tbuf: TaskBufferLike = None) -> None:
    """
    Process one daemon cycle for the given service.
    Call this from the service-specific entrypoint (daemon script or WatchDog).
    tbuf: an already-initialised TaskBuffer, or None to use the module-level singleton.
    """
    global _cycle_started_at
    cycle_started_at = time.monotonic()
    _cycle_started_at = cycle_started_at
    _logger.debug(f"start with service={service_name}")
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer as tbuf

    # keep this machine's liveness record current
    tbuf.upsert_machine_heartbeat(MY_HOSTNAME, service_name)

    # recover stale running rows from previous crashed cycles: this machine's own rows, plus
    # shared "any" rows orphaned by a crashed winner (any machine can reclaim them afterwards)
    tbuf.recover_stale_results(MY_HOSTNAME, max_processing_seconds=_STALE_THRESHOLD_SECONDS)
    tbuf.recover_stale_results(ANY_MACHINE, max_processing_seconds=_STALE_THRESHOLD_SECONDS)

    # find requests this machine should process
    cycle_seconds = _cycle_seconds(service_name)
    pending = tbuf.get_pending_requests_for_machine(MY_HOSTNAME, service_name, list(HANDLERS.keys()))
    # the requests that may run long after the others, each kind oldest first
    pending = sorted(pending, key=lambda row: row["request_type"] in _LONGEST_RUN_SECONDS)
    for row in pending:
        request_id = row["request_id"]
        request_type = row["request_type"]
        try:
            requester = json.loads(row["parameters"]).get("requester", "unknown")
        except json.JSONDecodeError:
            requester = "unknown"
        tmp_logger = LogWrapper(_logger, prefix=f"< request_id={request_id} >")
        handler = HANDLERS.get(request_type)
        if handler is None:
            tmp_logger.warning(f"unknown request_type={request_type}")
            continue
        if not _fits_in_cycle(request_type, cycle_seconds, cycle_started_at):
            tmp_logger.debug(f"left for the next cycle: a {request_type} may run {_LONGEST_RUN_SECONDS[request_type]} s, more than this cycle has left")
            continue
        # "any" requests share a single result row keyed by the sentinel, so the claim and the
        # result are written under ANY_MACHINE; single/all requests use this machine's hostname
        result_machine = ANY_MACHINE if row.get("machine_name") == ANY_MACHINE else MY_HOSTNAME
        if not tbuf.claim_async_result(request_id, result_machine):
            # another daemon instance (or, for "any", another machine) claimed it first
            continue
        tmp_logger.debug(f"processing type={request_type} request from '{requester}'")
        handler(row, tbuf, tmp_logger, result_machine)
    _logger.debug("done")
