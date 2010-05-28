'''
notifier

'''

import re
import sys
import fcntl
import commands
import threading
import urllib
import shelve
import smtplib
import datetime

from config import panda_config
from taskbuffer.OraDBProxy import DBProxy
from pandalogger.PandaLogger import PandaLogger
from dataservice.DDM import dq2Info

# logger
_logger = PandaLogger().getLogger('Notifier')

# lock file
_lockGetMail = open(panda_config.lockfile_getMail, 'w')

# ignored DN
_ignoreList = [
    'Nurcan Ozturk',
    'Xin Zhao',
    'Dietrich Liko',
    ]

# NG words in email address
_ngWordsInMailAddr = ['support','system','stuff','service','secretariat','club','user','admin',
                      'cvs','grid','librarian','svn','atlas','cms','lhcb','alice','alaelp']


class Notifier (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,job,datasets):
        threading.Thread.__init__(self)
        self.job = job
        self.datasets = datasets
        self.taskBuffer = taskBuffer


    # main
    def run(self):
        _logger.debug("%s start" % self.job.PandaID)
        try:
            # check job type
            if self.job.prodSourceLabel != 'user' and self.job.prodSourceLabel != 'panda':
                _logger.error("Invalid job type : %s" % self.job.prodSourceLabel)
                _logger.debug("%s end" % self.job.PandaID)
                return
            # ignore some DNs to avoid mail storm
            for igName in _ignoreList:
                if re.search(igName,self.job.prodUserID) != None:
                    _logger.debug("Ignore DN : %s" % self.job.prodUserID)
                    _logger.debug("%s end" % self.job.PandaID)
                    return
            # get e-mail address
            mailAddr = self.getEmail(self.job.prodUserID)
            if mailAddr == '':
                _logger.error("could not find email address for %s" % self.job.prodUserID)
                _logger.debug("%s end" % self.job.PandaID)
                return
            # not send 
            if mailAddr in ['notsend','',None]:
                _logger.debug("not send to %s" % self.job.prodUserID)
                _logger.debug("%s end" % self.job.PandaID)
                return
            # get IDs
            tmpIDs = self.taskBuffer.queryPandaIDwithDataset(self.datasets)
            ids = []
            for tmpID in tmpIDs:
                if not tmpID in ids:
                    ids.append(tmpID)
            _logger.debug("%s IDs: %s" % (self.job.PandaID,ids))
            if len(ids) != 0:
                # get job
                jobs = self.taskBuffer.peekJobs(ids,fromDefined=False,fromActive=False,fromWaiting=False)
                # statistics
                nTotal     = len(jobs)
                nSucceeded = 0
                nFailed    = 0
                nPartial   = 0
                nCancel    = 0
                # job ID
                jobID = self.job.jobDefinitionID
                # time info
                creationTime = self.job.creationTime
                endTime      = self.job.modificationTime
                if isinstance(endTime,datetime.datetime):
                    endTime = endTime.strftime('%Y-%m-%d %H:%M:%S')
                # site
                siteName = self.job.computingSite   
                # datasets
                iDSList = []
                oDSList = []
                for file in self.job.Files:
                    if file.type == 'input':
                        if not file.dataset in iDSList:
                            iDSList.append(file.dataset)
                    else:
                        if not file.dataset in oDSList:
                            oDSList.append(file.dataset)
                # count
                for job in jobs:
                    if job == None:
                        continue
                    if job.jobStatus == 'finished':
                        # check all files were used
                        allUses = True
                        for file in job.Files:
                            if file.type == 'input' and file.status in ['skipped']:
                                allUses = False
                                break
                        if allUses:
                            nSucceeded += 1
                        else:
                            nPartial += 1
                    elif job.jobStatus == 'failed':
                        nFailed += 1
                    elif job.jobStatus == 'cancelled':
                        nCancel += 1
                # make message
                fromadd = panda_config.emailSender
                message = \
"""Subject: PANDA notification for JobID : %s
From: %s
To: %s

Summary of JobID : %s

Created : %s (UTC)
Ended   : %s (UTC)

Site    : %s

Total Number of Jobs : %s
           Succeeded : %s
           Partial   : %s
           Failed    : %s
           Cancelled : %s
""" % (jobID,fromadd,mailAddr,jobID,creationTime,endTime,siteName,nTotal,
       nSucceeded,nPartial,nFailed,nCancel)
                for iDS in iDSList:
                    message += \
"""
In  : %s""" % iDS
                for oDS in oDSList:
                    message += \
"""
Out : %s""" % oDS
                urlData = {}
                urlData['job'] = '*'
                urlData['jobDefinitionID'] = jobID
                urlData['user'] = self.job.prodUserName
                message += \
"""

PandaMonURL : http://panda.cern.ch:25980/server/pandamon/query?%s
""" % urllib.urlencode(urlData)
                message += \
"""

Report Panda problems of any sort to

  the eGroup for help request
    hn-atlas-dist-analysis-help@cern.ch

  the Savannah for software bug
    https://savannah.cern.ch/projects/panda/
"""

                # send mail
                _logger.debug("%s send to %s\n%s" % (self.job.PandaID,mailAddr,message))
                server = smtplib.SMTP(panda_config.emailSMTPsrv)
                server.set_debuglevel(1)
                server.ehlo()
                server.starttls()
                server.login(panda_config.emailLogin,panda_config.emailPass)
                out = server.sendmail(fromadd,mailAddr,message)
                _logger.debug(out)
                server.quit()
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("%s %s" % (type,value))

        _logger.debug("%s end" % self.job.PandaID)


    # get email
    def getEmail(self,dn):
        # get DN
        _logger.debug("getDN for %s" % dn)
        dbProxy = DBProxy()
        distinguishedName = dbProxy.cleanUserID(dn)
        _logger.debug("DN = %s" % distinguishedName)
        if distinguishedName == "":
            _logger.error("cannot get DN for %s" % dn)
            return ""
        # get email from MetaDB
        mailAddr = self.taskBuffer.getEmailAddr(distinguishedName)
        if mailAddr == 'notsend':
            _logger.debug("email from MetaDB : '%s'" % mailAddr)
            return mailAddr
        # get email from DQ2
        realDN = re.sub('/CN=limited proxy','',dn)
        realDN = re.sub('(/CN=proxy)+','',realDN)
        try:
            _logger.debug("dq2Info.finger(%s)" % realDN)
            for iDDMTry in range(3):
                status,out = dq2Info.finger(realDN)
                if status != 0 or out.find("DQ2 internal server exception") != -1 \
                       or out.find("An error occurred on the central catalogs") != -1 \
                       or out.find("MySQL server has gone away") != -1:
                    time.sleep(10)
                else:
                    break
            _logger.debug(out)
            exec "userInfo=%s" % out
            mailAddr = userInfo['email']
            _logger.debug("email from DQ2 : '%s'" % mailAddr)            
            return mailAddr
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("%s %s" % (errType,errValue))
            return ""

                    
        
