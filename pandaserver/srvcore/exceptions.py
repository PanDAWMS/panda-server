"""
Dedicated exceptions for panda-server.
"""


# raised when file registration in DDM fails; fatal=True means non-retryable
class FileRegistrationError(Exception):
    def __init__(self, message: str = "", fatal: bool = False):
        super().__init__(message)
        self.fatal = fatal


# raised when Rucio rejects a subscription registration (e.g. invalid RSE expression)
class SubscriptionRegistrationError(Exception):
    pass


# raised when Rucio rejects a dataset location registration (invalid RSE expression / insufficient quota)
class DatasetLocationError(Exception):
    pass


# raised when the read-only account of the async db_read requests is not configured, or does not connect
class DBReaderUnavailableError(Exception):
    pass


# raised in an async db_read request when no time is left for the next round trip to the database
class DBReadDeadlineError(Exception):
    pass
