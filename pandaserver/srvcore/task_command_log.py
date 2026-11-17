"""
Shared pieces of the task command log, ATLAS_PANDA.TASKS_COMMANDLOG.

The API decorator (request_validation in pandaserver/api/v1/common.py) writes a row for each
task command it receives, before the command runs, and puts the row's key in
current_task_command while the command runs. The TaskBuffer methods that register a command
for JEDI in PRODSYS_COMM read the key there and pass it on to the DBProxy, which marks the
same row as queued in the transaction that registers the command. The key travels as an
explicit argument from TaskBuffer on, because the DBProxy may run in another process
(ConBridge), where this context is not seen.

The status of a row says where the command is:

    received --+-- the API responded ---------------------------> responded
               +-- the API ended with an exception -------------> error
               +-- registered for JEDI in PRODSYS_COMM ---------> queued --+-- JEDI took it ----------> accepted
                                                                           +-- JEDI rejected it ------> rejected
                                                                           +-- a later command came --> superseded

Each writer moves a row only from the status it expects, so the writes may land in any
order, e.g. JEDI may take a command before the API has recorded its response.
"""

import datetime
from contextvars import ContextVar
from typing import TypedDict

STATUS_RECEIVED = "received"
STATUS_RESPONDED = "responded"
STATUS_ERROR = "error"
STATUS_QUEUED = "queued"
STATUS_ACCEPTED = "accepted"
STATUS_REJECTED = "rejected"
STATUS_SUPERSEDED = "superseded"


class TaskCommandKey(TypedDict):
    """The key of the rows a request wrote: their COMMAND_ID and their COMMAND_TIME, the partition key."""

    command_id: int
    command_time: datetime.datetime


# the key of the rows for the task command the current API request is running, or None
current_task_command: ContextVar[TaskCommandKey | None] = ContextVar("current_task_command", default=None)
