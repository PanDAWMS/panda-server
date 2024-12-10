import sys
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger


# Base class for DB proxy modules
class BaseModule:
    # constructor
    def __init__(self, log_stream: PandaLogger):
        self._log_stream = log_stream
        self.conn = None
        self.cur = None

    # abstract method to commit
    def _commit(self):
        """
        Commit the transaction
        """
        raise NotImplementedError("commit is not implemented")

    # abstract method to rollback
    def _rollback(self):
        """
        Rollback the transaction
        """
        raise NotImplementedError("rollback is not implemented")

    # dump error message
    def dump_error_message(self, tmp_log: LogWrapper):
        """
        Dump error message to the log

        :param tmp_log: log wrapper
        """
        # error
        err_type, err_value = sys.exc_info()[:2]
        err_str = f"{err_type.__name__} {err_value}"
        err_str.strip()
        err_str += " "
        err_str += traceback.format_exc()
        tmp_log.error(err_str)

    # create method name and logger
    def create_method_name_logger(self, comment: str, tag: str = None) -> tuple[str, LogWrapper]:
        """
        Create method name and logger from function comment

        param comment: comment of the function
        param tag: tag to add to the method name
        return: (method name, log wrapper)
        """
        method_name = comment.split(" ")[-2].split(".")[-1]
        if tag is not None:
            method_name += f" < {tag} >"
        tmp_log = LogWrapper(self._log_stream, method_name)
        return method_name, tmp_log
