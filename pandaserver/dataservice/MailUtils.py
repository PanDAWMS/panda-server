'''
email utilities
'''

import sys
import smtplib

from config import panda_config
from pandalogger.PandaLogger import PandaLogger

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
            # make message
            fromAdd = panda_config.emailSender
            message = \
"""Subject: %s
From: %s
To: %s

%s
""" % (mailSubject,fromAdd,toAddr,mailBody)
            # send mail
            _logger.debug("send to %s\n%s" % (toAddr,message))
            server = smtplib.SMTP(panda_config.emailSMTPsrv)
            server.set_debuglevel(1)
            server.ehlo()
            server.starttls()
            server.login(panda_config.emailLogin,panda_config.emailPass)
            out = server.sendmail(fromAdd,toAddr,message)
            _logger.debug(out)
            server.quit()
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("%s %s" % (type,value))

        _logger.debug("end SEND session")
            

                    
        
