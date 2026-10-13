"""
Async request handler for the reads of the database, registered into processor.HANDLERS.

A db_read request holds a batch of queries, whose form is in
pandaserver.asyncprocess.db_read.request_shape. The queries run on the read-only account
of panda_config.dbuser_reader and never on the account of the task buffer the handler is
given, which only stores the result. Each request connects to the account and closes the
connection when it is done, so the account has a session only while a request runs. The
queries run in one read-only transaction, so every statement sees the database at the same
moment, and the answer gives that moment as as_of, on the clock of the session, which is UTC.

The stored result is {"success", "message", "data"}, with data {"as_of", "answers"} and one
answer per query, in order:

    {"status": "done", "truncated": bool, "tables": [{"table", "rows", "truncated", "probes",
        "seconds", "statement"}, ...]}
        Rows are objects keyed by the lower-case names the query returns. An answer is
        truncated when a table was cut at the limit or at the size of the result, or when an
        "in" took its values from a truncated answer: a cut set of keys must not look whole.
    {"status": "rejected", "rule": str, "message": str, "detail": {...}}
        The query broke a rule of request_shape, or result_size when the result had no room
        left for it, or batch_budget when the queries before it left too few of the index
        lookups the batch may make, BATCH_BUDGET_QUERIES times db_read_probe_budget.
    {"status": "deadline"}
        Not run, the batch having run out of time.
    {"status": "error", "code": str, "message": str}
        The database failed, with its ORA-, DPI-, DPY- or SQLSTATE code, or table_not_visible
        when the dictionary shows no columns of a table, which is what a missing grant looks
        like, or statement_timeout when a round trip ran over the limit of the connection,
        db_read_call_timeout, rather than over the time left: the query is too heavy and has to
        be narrowed. The batch stops at a database error, since a Postgres transaction takes no
        statement after one; the queries after it are errors with code not_run.

The batch has a deadline, before each round trip to the database the limit of that round
trip is lowered to the time left, and nothing runs past it. The deadline keeps MARGIN_SECONDS
back for storing the result, both from the daemon's timeout, which counts from the start of
the processing cycle, and from the age at which processor.run hands a running request out
again, which counts from the start of the request. When the daemon config has no enabled
entry running async_request_daemon, the daemon master does not run it, nor kill it: it was
started by hand, and only the age counts.
"""

import datetime
import decimal
import json
import time
import traceback
from typing import Any

from pandacommon.pandalogger.LogWrapper import LogWrapper

from pandaserver.asyncprocess import TaskBufferLike
from pandaserver.asyncprocess.db_read.compose import compose_statement
from pandaserver.asyncprocess.db_read.request_shape import (
    MAX_QUERIES,
    FromQuery,
    QueryRejected,
    check_query,
    parse_query,
)
from pandaserver.asyncprocess.db_read.schema_info import (
    STATEMENT_COMMENT,
    TableNotVisible,
    get_table_info,
)
from pandaserver.config import daemon_config, panda_config
from pandaserver.srvcore.exceptions import DBReadDeadlineError, DBReaderUnavailableError
from pandaserver.taskbuffer.db_proxy_mods.base_module import get_db_error_code
from pandaserver.taskbuffer.OraDBProxy import DBProxy

REQUEST_TYPE = "db_read"

# the daemon module that runs the requests submitted for the server
DAEMON_MODULE = "async_request_daemon"

# seconds kept back from the deadline for encoding and storing the result
MARGIN_SECONDS = 30

# the index lookups a whole batch may make, in budgets of one query: one heavy query leaves room
# for the others, and ten do not make ten times the work of one
BATCH_BUDGET_QUERIES = 3

# most bytes of the stored result, and the part of them kept for what is not rows or statements
RESULT_BYTES = 1_000_000
ENVELOPE_BYTES = 20_000

# codes of a round trip that ran out of time
_TIMEOUT_CODES = ("DPI-1067", "DPY-4024", "57014")

# most binds of a statement the log line shows, and most characters of a text bind it shows
LOG_BINDS = 20
LOG_TEXT_CHARS = 100


def _connect_reader() -> DBProxy:
    """
    Connect to the read-only account for one request.

    The daemon master does not start async_request_daemon while a run of it goes on, so a machine
    has at most one such connection, and only while a request runs. There is one attempt: the
    handler runs under a deadline, and the caller can submit again.

    Returns:
        DBProxy: the connected proxy, to be closed with cleanup()

    Raises:
        DBReaderUnavailableError: when panda_config has no dbuser_reader, or the connection fails
    """
    if not panda_config.dbuser_reader:
        raise DBReaderUnavailableError("dbuser_reader is not set in panda_server.cfg")
    proxy = DBProxy()
    if not proxy.connect(
        dbhost=panda_config.dbhost_reader or panda_config.dbhost,
        dbpasswd=panda_config.dbpasswd_reader or "",
        dbuser=panda_config.dbuser_reader,
        call_timeout_ms=panda_config.db_read_call_timeout * 1000,
    ):
        raise DBReaderUnavailableError(f"{panda_config.dbuser_reader} failed to connect")
    return proxy


def batch_deadline(request_started: float, cycle_started: float | None, daemon_timeout: int | None, stale_seconds: int) -> float:
    """
    Get the time by which a batch must be done.

    Args:
        request_started(float): time.monotonic() when the handler started
        cycle_started(float): time.monotonic() when the processing cycle started, or None when unknown
        daemon_timeout(int): seconds a cycle may run before the daemon master kills the worker, or None
            when no daemon master runs the cycle
        stale_seconds(int): seconds a request may run before processor.run hands it out again

    Returns:
        float: the deadline, on the clock of time.monotonic()
    """
    deadline = request_started + stale_seconds
    if daemon_timeout is not None:
        cycle = request_started if cycle_started is None else cycle_started
        deadline = min(cycle + daemon_timeout, deadline)
    return deadline - MARGIN_SECONDS


def encode_value(value: Any) -> Any:
    """
    Get a value of a row as JSON can hold it, times the way the API writes them.

    Args:
        value(Any): the value the driver gives

    Returns:
        Any: the value for json.dumps
    """
    if isinstance(value, (datetime.datetime, datetime.date)):
        return {"__datetime__": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if hasattr(value, "read"):
        # a LOB the output type handler leaves alone, such as a BLOB
        return encode_value(value.read())
    return value


def _rejected(rejection: QueryRejected) -> dict[str, Any]:
    return {"status": "rejected", "rule": rejection.rule, "message": rejection.message, "detail": rejection.detail}


class _Batch:
    """The queries of one request, on one connection of the read-only account."""

    def __init__(self, proxy: DBProxy, deadline: float, budget: int, tmp_logger: LogWrapper) -> None:
        self.proxy = proxy
        self.backend = proxy.backend
        self.deadline = deadline
        self.budget = budget
        self.probes_left = BATCH_BUDGET_QUERIES * budget
        self.tmp_logger = tmp_logger
        self.bytes_left = RESULT_BYTES - ENVELOPE_BYTES
        # whether the limit of the last round trip was the time left rather than the connection's own
        self.limited_by_deadline = False
        # the answers so far, and for each the names and the rows, as the driver gave them, of each table
        self.answers: list[dict[str, Any]] = []
        self.rows: list[list[tuple[tuple[str, ...], list[tuple[Any, ...]]]]] = []
        # the answer the queries after a stop get, once something has stopped the batch
        self.stopped: dict[str, Any] | None = None
        # the moment the transaction sees, before which the partitions a query is charged for start
        self.as_of: datetime.datetime | None = None
        # for the log line: the binds of each table of each answer, None for a table not read, and the
        # seconds the start of the transaction and the reads of the dictionary took
        self.binds: list[list[dict[str, Any] | None]] = []
        self.start_seconds = 0.0
        self.dictionary_seconds = 0.0

    def limit_round_trip(self) -> None:
        left_ms = int((self.deadline - time.monotonic()) * 1000)
        if left_ms < 1:
            raise DBReadDeadlineError()
        cap_ms = self.proxy.call_timeout_ms
        self.limited_by_deadline = cap_ms is None or left_ms < cap_ms
        self.proxy.set_call_timeout(left_ms)

    def round_trip(self, sql: str, binds: dict[str, Any]) -> list[tuple[Any, ...]]:
        self.limit_round_trip()
        self.proxy.cur.execute(sql, binds)
        rows: list[tuple[Any, ...]] = self.proxy.cur.fetchall()
        return rows

    def start(self) -> datetime.datetime:
        # end whatever the connection had open, so that the read-only transaction is the next one
        self.proxy.conn.rollback()
        if self.backend == "postgres":
            sql = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        else:
            sql = "SET TRANSACTION READ ONLY"
        # the first statement of the transaction, run under the limit of the connection: on
        # Postgres the SET LOCAL a lower limit takes is a statement of its own
        self.proxy.cur.execute(sql + STATEMENT_COMMENT, {})
        self.limit_round_trip()
        return self.proxy.get_db_utc_now()

    def rollback(self) -> None:
        # the connection is closed after the request, so a rollback is all an error or the end needs
        try:
            self.proxy.conn.rollback()
        except Exception:
            self.tmp_logger.warning(f"failed to roll back: {traceback.format_exc()}")

    def values_of(self, reference: FromQuery) -> list[Any]:
        # the values of a column over every table of an answer, each once, nulls left out
        values: dict[Any, None] = {}
        for names, rows in self.rows[reference.query]:
            if reference.column in names:
                index = names.index(reference.column)
                values.update((row[index], None) for row in rows if row[index] is not None)
        return list(values)

    def answer(self, position: int, raw: Any) -> dict[str, Any]:
        try:
            query = parse_query(raw, position)
        except QueryRejected as rejection:
            return _rejected(rejection)
        # the values of each "in" from an earlier query
        from_values: dict[FromQuery, list[Any]] = {}
        from_truncated = False
        for reference in query.from_queries():
            source = self.answers[reference.query]
            if source["status"] != "done":
                # say why, since a query rejected or failed is not one that found nothing
                message = f"query {reference.query} has no rows to take values from, being {source['status']}"
                return _rejected(QueryRejected("from_query", message, query=reference.query, status=source["status"]))
            if not any(reference.column in names for names, _ in self.rows[reference.query]):
                return _rejected(QueryRejected("from_query", f"query {reference.query} does not return {reference.column}", query=reference.query))
            from_values[reference] = self.values_of(reference)
            from_truncated = from_truncated or source["truncated"]
        dictionary_started = time.monotonic()
        try:
            infos = {table.name: get_table_info(self.round_trip, self.backend, table.owner, table.name) for table in query.tables}
        except TableNotVisible as error:
            return {"status": "error", "code": "table_not_visible", "message": f"the dictionary shows no columns of {error}"}
        finally:
            self.dictionary_seconds += time.monotonic() - dictionary_started
        try:
            plans = check_query(query, infos, {reference: len(values) for reference, values in from_values.items()}, self.budget, now=self.as_of)
        except QueryRejected as rejection:
            return _rejected(rejection)
        # charged before it runs, a query that fails having cost the database all the same
        cost = sum(plan.probes for plan in plans)
        if cost > self.probes_left:
            message = f"the query costs {cost} index lookups, over the {self.probes_left} left of the batch's {BATCH_BUDGET_QUERIES * self.budget}"
            return _rejected(QueryRejected("batch_budget", message, probes=cost, left=self.probes_left, budget=BATCH_BUDGET_QUERIES * self.budget))
        self.probes_left -= cost
        tables: list[dict[str, Any]] = []
        raw_tables: list[tuple[tuple[str, ...], list[tuple[Any, ...]]]] = []
        truncated = from_truncated
        for plan in plans:
            statement = compose_statement(plan, query, from_values, self.backend)
            if statement is None:
                # an "in" from a query with no values, which no row satisfies
                tables.append({"table": plan.table.name, "rows": [], "truncated": False, "probes": 0, "seconds": 0.0, "statement": None})
                self.binds[position].append(None)
                continue
            if len(statement.sql) > self.bytes_left:
                self.stopped = {"status": "rejected", "rule": "result_size", "message": "the result has no room left", "detail": {}}
                return dict(self.stopped)
            self.bytes_left -= len(statement.sql)
            self.proxy.cur.arraysize = statement.limit + 1
            started = time.monotonic()
            rows = self.round_trip(statement.sql, statement.binds)
            seconds = round(time.monotonic() - started, 3)
            cut = len(rows) > statement.limit
            encoded: list[dict[str, Any]] = []
            for row in rows[: statement.limit]:
                item = {name: encode_value(value) for name, value in zip(statement.names, row)}
                size = len(json.dumps(item)) + 2
                if size > self.bytes_left:
                    cut = True
                    self.stopped = {"status": "rejected", "rule": "result_size", "message": "the result has no room left", "detail": {}}
                    break
                self.bytes_left -= size
                encoded.append(item)
            raw_tables.append((statement.names, rows[: len(encoded)]))
            truncated = truncated or cut
            tables.append({"table": plan.table.name, "rows": encoded, "truncated": cut, "probes": plan.probes, "seconds": seconds, "statement": statement.sql})
            self.binds[position].append(statement.binds)
            if self.stopped is not None:
                # the tables after this one are not read, which the truncated answer says
                break
        self.rows[position] = raw_tables
        return {"status": "done", "truncated": truncated, "tables": tables}

    def run(self, raw_queries: list[Any]) -> dict[str, Any]:
        started = time.monotonic()
        as_of = self.as_of = self.start()
        self.start_seconds = time.monotonic() - started
        for position, raw in enumerate(raw_queries):
            self.rows.append([])
            self.binds.append([])
            if self.stopped is not None:
                self.answers.append(dict(self.stopped))
                continue
            try:
                answer = self.answer(position, raw)
            except DBReadDeadlineError:
                answer = {"status": "deadline"}
                self.stopped = answer
            except Exception as error:
                code = get_db_error_code(error)
                if code in _TIMEOUT_CODES and self.limited_by_deadline:
                    answer = {"status": "deadline"}
                    self.stopped = answer
                elif code in _TIMEOUT_CODES:
                    # the connection's own limit, so the query and not the batch is to blame
                    limit = (self.proxy.call_timeout_ms or 0) / 1000
                    self.tmp_logger.warning(f"query {position} ran over the limit of {limit:g} s of a round trip with {code}")
                    message = f"a round trip ran over the limit of {limit:g} s ({code}): narrow the query with a window on the partition key or fewer keys"
                    answer = {"status": "error", "code": "statement_timeout", "message": message}
                    self.stopped = {"status": "error", "code": "not_run", "message": f"not run after the timeout of query {position}"}
                else:
                    self.tmp_logger.error(f"query {position} failed with {code}: {traceback.format_exc()}")
                    answer = {"status": "error", "code": code, "message": str(error)[:1000]}
                    self.stopped = {"status": "error", "code": "not_run", "message": f"not run after the error of query {position}"}
                self.rollback()
            self.answers.append(answer)
        self.rollback()
        return {"as_of": encode_value(as_of), "answers": self.answers}


def _binds_for_log(binds: dict[str, Any] | None) -> dict[str, Any] | None:
    # the first binds of a statement, a long text cut short, since an "in" may have a thousand
    if binds is None:
        return None
    shown: dict[str, Any] = {}
    for name, value in list(binds.items())[:LOG_BINDS]:
        value = encode_value(value)
        if isinstance(value, str) and len(value) > LOG_TEXT_CHARS:
            value = value[:LOG_TEXT_CHARS] + "..."
        shown[name] = value
    if len(binds) > LOG_BINDS:
        shown["..."] = f"{len(binds) - LOG_BINDS} more"
    return shown


def _summary(answers: list[dict[str, Any]], binds: list[list[dict[str, Any] | None]]) -> list[dict[str, Any]]:
    # what the log line keeps of each answer: its status and why, and the statement, binds, cost and time of each table
    summary: list[dict[str, Any]] = []
    for answer, answer_binds in zip(answers, binds):
        item: dict[str, Any] = {key: answer[key] for key in ("status", "rule", "code", "message", "detail") if key in answer}
        if "tables" in answer:
            item["tables"] = [
                {key: table[key] for key in ("table", "probes", "seconds", "truncated")}
                | {"rows": len(table["rows"]), "statement": table["statement"], "binds": _binds_for_log(table_binds)}
                for table, table_binds in zip(answer["tables"], answer_binds)
            ]
        summary.append(item)
    return summary


def _run(parameters: dict[str, Any], started: float, tmp_logger: LogWrapper) -> tuple[bool, str, dict[str, Any] | None]:
    queries = parameters.get("queries")
    if not isinstance(queries, list) or not 1 <= len(queries) <= MAX_QUERIES:
        return False, f"queries must be a list of 1 to {MAX_QUERIES} queries", None
    if panda_config.backend not in ("oracle", "postgres"):
        return False, f"db_read is not available with {panda_config.backend}", None
    # imported here since processor imports this module to register the handler
    from pandaserver.asyncprocess import processor

    daemon = daemon_config.find_daemon(DAEMON_MODULE)
    if daemon is None:
        # the daemon master runs only the enabled entries, reading them as find_daemon does
        daemon_timeout = None
        tmp_logger.info(f"no enabled {DAEMON_MODULE} in the daemon config, so run by hand with no timeout but the age of the request")
    else:
        daemon_timeout = daemon_config.get_timeout(daemon)
    deadline = batch_deadline(started, processor.get_cycle_started_at(), daemon_timeout, processor._STALE_THRESHOLD_SECONDS)
    if deadline <= time.monotonic():
        return (
            False,
            f"no time left for the queries: {DAEMON_MODULE} has a timeout of {daemon_timeout} s, {MARGIN_SECONDS} s of which are kept "
            "for the result, and the cycle may have used the rest on other requests",
            None,
        )
    connect_started = time.monotonic()
    try:
        proxy = _connect_reader()
    except DBReaderUnavailableError as error:
        return False, f"the read-only account is not available: {error}", None
    connect_seconds = time.monotonic() - connect_started
    batch = _Batch(proxy, deadline, panda_config.db_read_probe_budget, tmp_logger)
    try:
        try:
            data = batch.run(queries)
        except Exception as error:
            # only the start of the transaction gets here, every query catching its own errors
            code = get_db_error_code(error)
            tmp_logger.error(f"failed to start the read-only transaction with {code}: {traceback.format_exc()}")
            return False, f"the read-only transaction could not start: {code}", None
    finally:
        close_started = time.monotonic()
        proxy.cleanup()
        close_seconds = time.monotonic() - close_started
    # where the time went besides the statements, whose times are in the summary
    steps = {"connect": connect_seconds, "start": batch.start_seconds, "dictionary": batch.dictionary_seconds, "close": close_seconds}
    seconds = round(time.monotonic() - started, 3)
    tmp_logger.info(
        f"db_read by {parameters.get('requester')} in {seconds} s ({', '.join(f'{name} {value:.3f} s' for name, value in steps.items())}): "
        f"{json.dumps(_summary(data['answers'], batch.binds), default=str)}"
    )
    return True, "", data


def _handle_db_read(row: dict[str, Any], tb: TaskBufferLike, tmp_logger: LogWrapper, result_machine: str) -> None:
    """
    Run the queries of a db_read request and store the answers as its result.

    Args:
        row(dict): the async_requests row to process
        tb(TaskBuffer): task buffer, used only to store the result
        tmp_logger(LogWrapper): logger of the processing cycle
        result_machine(str): machine_name the result row is keyed by
    """
    request_id = row["request_id"]
    started = time.monotonic()
    try:
        parameters = json.loads(row["parameters"] or "{}")
        success, message, data = _run(parameters, started, tmp_logger)
        result = json.dumps({"success": success, "message": message, "data": data})
    except Exception:
        # processor.run does not catch what a handler raises, so this is where a crash has to end:
        # left to it, the request would stay running until handed out again. Running it again
        # would only crash the same way
        err_msg = traceback.format_exc()
        tmp_logger.error(f"db_read failed with {err_msg}")
        tb.finish_async_result(request_id, result_machine, "failed", error_msg=err_msg, retriable=False)
        return
    if not success:
        # what keeps a whole request from running is the server's cfg, its daemon config or its database,
        # since submission has checked the batch, so the operators get it here as well as the caller
        tmp_logger.error(f"db_read by {parameters.get('requester')} not run: {message}")
    tb.finish_async_result(request_id, result_machine, "done", result=result)


HANDLERS = {REQUEST_TYPE: _handle_db_read}
