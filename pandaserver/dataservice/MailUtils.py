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
                if not tmpToAddr in listToAddr:
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
            

    # send update notification to user
    def sendSiteAccessUpdate(self,toAddr,newStatus,pandaSite):
        # subject
        mailSubject = "PANDA Update on Access Request for %s" % pandaSite
        # message
        mailBody = "Hello,\n\nYour access request for %s has been %s \n" % (pandaSite,newStatus.upper())
        # send
        retVal = self.send(toAddr,mailSubject,mailBody)
        # return
        return retVal


    # send requests to cloud responsible
    def sendSiteAccessRequest(self,toAddr,requestsMap,cloud):
        # subject
        mailSubject = "PANDA Access Requests in %s" % cloud
        # message
        mailBody = "Hello,\n\nThere are access requests to be approved or rejected.\n\n"
        for pandaSite in requestsMap:
            userNames = requestsMap[pandaSite]
            mailBody += "   %s\n" % pandaSite
            userStr = ''
            for userName in userNames:
                userStr += ' %s,' % userName
            userStr = userStr[:-1]
            mailBody += "       %s\n\n" % userStr
        # send
        retVal = self.send(toAddr,mailSubject,mailBody)
        # return
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
