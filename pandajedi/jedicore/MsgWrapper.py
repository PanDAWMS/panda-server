import logging
import re
from typing import Any

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jediconfig import jedi_config
from pandaserver.userinterface import Client


class MsgWrapper:
    def __init__(self, logger: logging.Logger, token: str | None = None, lineLimit: int = 500, monToken: str | None = None) -> None:
        self.logger = logger
        # use timestamp as token if undefined
        if token is None:
            self.token = f"<{naive_utcnow().isoformat('/')}>"
        else:
            self.token = token
        # token for http logger
        if monToken is None:
            self.monToken = self.token
        else:
            self.monToken = monToken
        # remove <> for django
        try:
            self.monToken = re.sub("<(?P<name>[^>]+)>", r"\g<name>", self.monToken)
        except Exception:
            pass
        # message buffer
        self.msgBuffer: list[str] = []
        self.bareMsg: list[str] = []
        self.lineLimit = lineLimit
        # how many more messages keepMsg() will store, or None for no limit
        self.message_slot: int | None = None

    def keepMsg(self, msg: str) -> None:
        # check if message slot is defined and available
        if self.message_slot is not None:
            if self.message_slot < 0:
                return
            if self.message_slot == 0:
                msg = "   ..."
            self.message_slot -= 1
        # keep max message depth
        if len(self.msgBuffer) > self.lineLimit:
            self.msgBuffer.pop(0)
            self.bareMsg.pop(0)
        timeNow = naive_utcnow()
        self.msgBuffer.append(f"{timeNow.isoformat(' ')} : {msg}")
        self.bareMsg.append(msg)

    def set_message_slot(self, slot: int = 10) -> None:
        self.message_slot = slot

    def unset_message_slot(self) -> None:
        self.message_slot = None

    # the four levels below stringify whatever they are handed, which is what lets the
    # callers pass a None error diagnostic or a decoded task parameter straight in
    def info(self, msg: Any) -> None:
        msg = str(msg)
        self.logger.info(self.token + " " + msg)
        self.keepMsg(msg)

    def debug(self, msg: Any) -> None:
        msg = str(msg)
        self.logger.debug(self.token + " " + msg)

    def error(self, msg: Any) -> None:
        msg = str(msg)
        self.logger.error(self.token + " " + msg)
        self.keepMsg(msg)

    def warning(self, msg: Any) -> None:
        msg = str(msg)
        self.logger.warning(self.token + " " + msg)
        self.keepMsg(msg)

    def dumpToString(self) -> str:
        strMsg = ""
        for msg in self.msgBuffer:
            strMsg += msg
            strMsg += "\n"
        return strMsg

    # returns the text the caller puts in the task error dialog, which is either a link
    # to the uploaded log or the reason there is none
    def uploadLog(self, id: int | None) -> str:
        strMsg = self.dumpToString()
        # every caller passes a task ID off a task spec, whose column reads None until the
        # spec is loaded. Client.uploadLog stringifies the name anyway, so this is only
        # doing it one step earlier
        s, o = Client.uploadLog(strMsg, str(id))
        if s != 0:
            return f"failed to upload log with {s} {o}."

        success = o["success"]
        message: str = o["message"]
        url = o["data"]

        if success and url.startswith("http"):
            return f'<a href="{url}">log</a> : {". ".join(self.bareMsg[-2:])}.'

        return message

    # send message to logger
    def sendMsg(self, message: str, msgType: str, msgLevel: str = "info", escapeChar: bool = False) -> None:
        try:
            # get logger
            tmpPandaLogger = PandaLogger()
            # lock HTTP handler
            tmpPandaLogger.lock()
            tmpPandaLogger.setParams({"Type": msgType})
            # get logger
            tmpLogger = tmpPandaLogger.getHttpLogger(jedi_config.master.loggername)
            # escape special characters
            if escapeChar:
                message = message.replace("<", "&lt;")
                message = message.replace(">", "&gt;")
            # add message
            message = self.monToken + " " + message
            if msgLevel == "error":
                tmpLogger.error(message)
            elif msgLevel == "warning":
                tmpLogger.warning(message)
            elif msgLevel == "info":
                tmpLogger.info(message)
            else:
                tmpLogger.debug(message)
            # release HTTP handler
            tmpPandaLogger.release()
        except Exception:
            pass
