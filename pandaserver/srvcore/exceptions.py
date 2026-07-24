"""
Dedicated exceptions for panda-server.
"""


# raised when file registration in DDM fails; fatal=True means non-retryable
class FileRegistrationError(Exception):
    def __init__(self, message: str = "", fatal: bool = False):
        super().__init__(message)
        self.fatal = fatal
