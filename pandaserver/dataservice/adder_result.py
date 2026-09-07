from typing import Any

"""
Class to represent the result of the Adder operation

"""


class AdderResult:
    """
    Class to represent the result of the Adder operation.
    """

    # slot
    __slot__ = ("statusCode", "transferringFiles")

    # return code
    rc_succeeded = 0
    rc_temporary = 1
    rc_fatal = 2

    # constructor
    def __init__(self) -> None:
        """
        Initialize the AdderResult with default values.
        """
        # result of interactions with data management system (DMS)
        self.status_code: int | None = None

        # list of files which are being transferred by DMS
        self.transferring_files: list[Any] = []

        # list of files which are being merged
        self.merging_files: list[Any] = []

    # succeeded
    def set_succeeded(self) -> None:
        """
        Set the status to succeeded.
        """
        self.status_code = self.rc_succeeded

    # temporary error to retry later
    def set_temporary(self) -> None:
        """
        Set the status to temporary error.
        """
        self.status_code = self.rc_temporary

    # fatal error
    def set_fatal(self) -> None:
        """
        Set the status to fatal.
        """
        self.status_code = self.rc_fatal

    # check if succeeded
    def is_succeeded(self) -> bool:
        """
        Check if the status is 'succeeded'.
        True if status is 'succeeded', False otherwise.
        """
        return self.status_code == self.rc_succeeded

    # check if temporary
    def is_temporary(self) -> bool:
        """
        Check if the status is 'temporary'.
        True if status is 'temporary', False otherwise.
        """
        return self.status_code == self.rc_temporary

    # check if fatal error
    def is_fatal(self) -> bool:
        """
        Check if the status is 'fatal'.
        True if status is 'fatal', False otherwise.
        """
        return self.status_code == self.rc_fatal
