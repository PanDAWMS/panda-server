'''
email utilities
'''

import sys
import smtplib

from pandaserver.config import panda_config
from pandacommon.pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('MailUtils')

class MailUtils:
    # constructor
    def __init__(self):
        pass

    # main
    def send(self,toAddr,mailSubject,mailBody):
        _logger.debug("start SEND session")
        try:
            # remove duplicated address
            listToAddr = []
            newToAddr = ''
            for tmpToAddr in toAddr.split(','):
                if tmpToAddr not in listToAddr:
                    listToAddr.append(tmpToAddr)
                    newToAddr += '%s,' % tmpToAddr
            toAddr = newToAddr[:-1]
            # make message
            fromAdd = panda_config.emailSender
            message = \
"""Subject: %s
From: %s
To: %s

%s
""" % (mailSubject,fromAdd,toAddr,mailBody)
            message = self.addTailer(message)
            # send mail
            _logger.debug("send to %s\n%s" % (toAddr,message))
            server = smtplib.SMTP(panda_config.emailSMTPsrv)
            server.set_debuglevel(1)
            server.ehlo()
            server.starttls()
            #server.login(panda_config.emailLogin,panda_config.emailPass)
            out = server.sendmail(fromAdd,listToAddr,message)
            _logger.debug(out)
            server.quit()
            retVal = True
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error("%s %s" % (type,value))
            retVal = False
        _logger.debug("end SEND session")
        return retVal


    # add tailer
    def addTailer(self,msg):
        msg += """
Report Panda problems of any sort to

  the eGroup for help request
    hn-atlas-dist-analysis-help@cern.ch

  the Panda JIRA for software bug
    https://its.cern.ch/jira/browse/ATLASPANDA
"""
        return msg
