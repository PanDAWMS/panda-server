"""
email utilities
"""

import smtplib
import sys

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config

# logger
_logger = PandaLogger().getLogger("MailUtils")


# wrapper to patch smtplib.stderr to send debug info to logger
class StderrLogger(object):
    def __init__(self, tmpLog):
        self.tmpLog = tmpLog

    def write(self, message):
        message = message.strip()
        if message != "":
            self.tmpLog.debug(message)


# wrapper of SMTP to redirect messages
class MySMTP(smtplib.SMTP):
    def set_log(self, tmp_log):
        self.tmpLog = tmp_log
        try:
            self.org_stderr = getattr(smtplib, "stderr")
            setattr(smtplib, "stderr", tmp_log)
        except Exception:
            self.org_stderr = None

    def _print_debug(self, *args):
        self.tmpLog.write(" ".join(map(str, args)))

    def reset_log(self):
        if self.org_stderr is not None:
            setattr(smtplib, "stderr", self.org_stderr)


class MailUtils:
    # constructor
    def __init__(self):
        pass

    # main
    def send(self, toAddr, mailSubject, mailBody):
        _logger.debug("start SEND session")
        try:
            # remove duplicated address
            listToAddr = []
            newToAddr = ""
            for tmpToAddr in toAddr.split(","):
                if tmpToAddr not in listToAddr:
                    listToAddr.append(tmpToAddr)
                    newToAddr += f"{tmpToAddr},"
            toAddr = newToAddr[:-1]

            # make message
            fromAdd = panda_config.emailSender
            message = f"Subject: {mailSubject}\n" f"From: {fromAdd}\n" f"To: {toAddr}\n\n" f"{mailBody}"
            message = self.addTailer(message)

            # send mail
            _logger.debug(f"send to {toAddr}\n{message}")
            stderrLog = StderrLogger(_logger)
            server = MySMTP(panda_config.emailSMTPsrv)
            server.set_debuglevel(1)
            server.set_log(stderrLog)
            server.ehlo()
            server.starttls()
            # server.login(panda_config.emailLogin,panda_config.emailPass)
            out = server.sendmail(fromAdd, listToAddr, message)
            _logger.debug(out)
            server.quit()
            retVal = True
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error(f"{type} {value}")
            retVal = False
        try:
            server.reset_log()
        except Exception:
            pass
        _logger.debug("end SEND session")
        return retVal

    # add tailer
    def addTailer(self, msg):
        msg += """
Report Panda problems of any sort to

  the eGroup for help request
    hn-atlas-dist-analysis-help@cern.ch
    
  the Discourse forum for distributed computing help  
    https://atlas-talk.web.cern.ch/c/distributed-computing-help
    
  the Panda JIRA for software bug
    https://its.cern.ch/jira/browse/ATLASPANDA
"""
        return msg
