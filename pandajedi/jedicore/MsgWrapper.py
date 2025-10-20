import datetime
import re
import time

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jediconfig import jedi_config
from pandaserver.userinterface import Client


class MsgWrapper:
    def __init__(self, logger, token=None, lineLimit=500, monToken=None):
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
            self.monToken = re.sub("<(?P<name>[^>]+)>", "\g<name>", self.monToken)
        except Exception:
            pass
        # message buffer
        self.msgBuffer = []
        self.bareMsg = []
        self.lineLimit = lineLimit

    def keepMsg(self, msg):
        # keep max message depth
        if len(self.msgBuffer) > self.lineLimit:
            self.msgBuffer.pop(0)
            self.bareMsg.pop(0)
        timeNow = naive_utcnow()
        self.msgBuffer.append(f"{timeNow.isoformat(' ')} : {msg}")
        self.bareMsg.append(msg)

    def info(self, msg):
        msg = str(msg)
        self.logger.info(self.token + " " + msg)
        self.keepMsg(msg)

    def debug(self, msg):
        msg = str(msg)
        self.logger.debug(self.token + " " + msg)
        self.keepMsg(msg)

    def error(self, msg):
        msg = str(msg)
        self.logger.error(self.token + " " + msg)
        self.keepMsg(msg)

    def warning(self, msg):
        msg = str(msg)
        self.logger.warning(self.token + " " + msg)
        self.keepMsg(msg)

    def dumpToString(self):
        strMsg = ""
        for msg in self.msgBuffer:
            strMsg += msg
            strMsg += "\n"
        return strMsg

    def uploadLog(self, id):
        strMsg = self.dumpToString()
        s, o = Client.uploadLog(strMsg, id)
        if s != 0:
            return f"failed to upload log with {s} {o}."

        success = o["success"]
        message = o["message"]
        url = o["data"]

        if success and url.startswith("http"):
            return f"<a href=\"{url}\">log</a> : {'. '.join(self.bareMsg[-2:])}."

        return message

    # send message to logger
    def sendMsg(self, message, msgType, msgLevel="info", escapeChar=False):
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
