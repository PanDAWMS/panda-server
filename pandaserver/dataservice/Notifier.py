'''
notifier

'''

import re
import sys
import uuid
import urllib
import smtplib
import datetime
import traceback
import time

from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.dataservice.DDM import rucioAPI
import pandaserver.taskbuffer.ErrorCode

# logger
_logger = PandaLogger().getLogger('Notifier')

# ignored DN
_ignoreList = [
    'Nurcan Ozturk',
    'Xin Zhao',
    'Dietrich Liko',
    ]

# NG words in email address
_ngWordsInMailAddr = ['support','system','stuff','service','secretariat','club','user','admin',
                      'cvs','grid','librarian','svn','atlas','cms','lhcb','alice','alaelp']

# port for SMTP server
smtpPortList = [25,587]

def initLogger(pLogger):
    # redirect logging to parent as it doesn't work in nested threads
    global _logger
    _logger = pLogger


# wrapper to patch smtplib.stderr to send debug info to logger
class StderrLogger(object):
    def __init__(self,token):
        self.token  = token
    def write(self,message):
        message = message.strip()
        if message != '':
            _logger.debug('%s %s' % (self.token,message))


class Notifier:
    # constructor
    def __init__(self,taskBuffer,job,datasets,summary={},mailFile=None,mailFileName=''):
        self.job = job
        self.datasets = datasets
        self.taskBuffer = taskBuffer
        self.summary = summary
        self.mailFile = mailFile
        self.mailFileName = mailFileName

    # main
    def run(self):
        if self.mailFile is None:
            _logger.debug("%s start" % self.job.PandaID)
            try:
                # check job type
                if self.job.prodSourceLabel != 'user' and self.job.prodSourceLabel != 'panda':
                    _logger.error("Invalid job type : %s" % self.job.prodSourceLabel)
                    _logger.debug("%s end" % self.job.PandaID)
                    return
                # ignore some DNs to avoid mail storm
                for igName in _ignoreList:
                    if re.search(igName,self.job.prodUserID) is not None:
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
                if mailAddr in ['notsend','',None] or (mailAddr is not None and mailAddr.startswith('notsend')):
                    _logger.debug("not send to %s" % self.job.prodUserID)
                    _logger.debug("%s end" % self.job.PandaID)
                    return
                # use all datasets
                if self.summary != {}:
                    self.datasets = []
                    for tmpJobID in self.summary:
                        tmpDsList = self.summary[tmpJobID]
                        if tmpDsList == []:
                            continue
                        self.datasets += tmpDsList
                # get full jobSpec including metadata
                self.job = self.taskBuffer.peekJobs([self.job.PandaID],fromDefined=False,
                                    fromActive=False,fromWaiting=False)[0]
                if self.job is None:
                    _logger.error('%s : not found in DB' % self.job.PandaID)
                    _logger.debug("%s end" % self.job.PandaID)
                    return
                # get IDs
                ids = []
                # from active tables
                tmpIDs = self.taskBuffer.queryPandaIDwithDataset(self.datasets)
                for tmpID in tmpIDs:
                    if tmpID not in ids:
                        ids.append(tmpID)
                # from archived table
                if self.job.jobsetID in [0,'NULL',None]:
                    tmpIDs = self.taskBuffer.getPandIDsWithIdInArch(self.job.prodUserName,self.job.jobDefinitionID,False)
                else:
                    tmpIDs = self.taskBuffer.getPandIDsWithIdInArch(self.job.prodUserName,self.job.jobsetID,True)
                for tmpID in tmpIDs:
                    if tmpID not in ids:
                        ids.append(tmpID)
                _logger.debug("%s IDs: %s" % (self.job.PandaID,ids))
                if len(ids) != 0:
                    # get jobs
                    jobs = self.taskBuffer.getFullJobStatus(ids,fromDefined=False,fromActive=False,
                                                            fromWaiting=False,forAnal=False)
                    # statistics
                    nTotal     = 0
                    nSucceeded = 0
                    nFailed    = 0
                    nPartial   = 0
                    nCancel    = 0
                    # time info
                    creationTime = self.job.creationTime
                    endTime      = self.job.modificationTime
                    if isinstance(endTime,datetime.datetime):
                        endTime = endTime.strftime('%Y-%m-%d %H:%M:%S')
                    # datasets
                    iDSList = []
                    oDSList = []
                    siteMap = {}
                    logDS = None
                    for tmpJob in jobs:
                        if tmpJob.jobDefinitionID not in siteMap:
                            siteMap[tmpJob.jobDefinitionID] = tmpJob.computingSite
                        for file in tmpJob.Files:
                            if file.type == 'input':
                                if file.dataset not in iDSList:
                                    iDSList.append(file.dataset)
                            else:
                                if file.dataset not in oDSList:
                                    oDSList.append(file.dataset)
                                if file.type == 'log':
                                    logDS = file.dataset
                    # job/jobset IDs and site
                    if self.summary == {}:
                        jobIDsite = "%s/%s" % (self.job.jobDefinitionID,self.job.computingSite)
                        jobsetID = self.job.jobDefinitionID
                        jobDefIDList = [self.job.jobDefinitionID]
                    else:
                        jobDefIDList = list(self.summary)
                        jobDefIDList.sort()
                        jobIDsite = ''
                        tmpIndent = "             "
                        for tmpJobID in jobDefIDList:
                            jobIDsite += '%s/%s\n%s' % (tmpJobID,siteMap[tmpJobID],tmpIndent)
                        remCount = len(tmpIndent) + 1
                        jobIDsite = jobIDsite[:-remCount]
                        jobsetID = self.job.jobsetID
                    # count
                    for job in jobs:
                        if job is None:
                            continue
                        # ignore pilot-retried job
                        if job.taskBufferErrorCode in [pandaserver.taskbuffer.ErrorCode.EC_PilotRetried]:
                            continue
                        # total
                        nTotal += 1
                        # count per job status
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
                    if nSucceeded == nTotal:
                        finalStatInSub = "(All Succeeded)"
                    else:
                        finalStatInSub = "(%s/%s Succeeded)" % (nSucceeded,nTotal)
                    fromadd = panda_config.emailSender
                    if self.job.jobsetID in [0,'NULL',None]:
                        message = \
"""Subject: PANDA notification for JobID : %s  %s
From: %s
To: %s

Summary of JobID : %s

Site : %s""" % (self.job.jobDefinitionID,finalStatInSub,fromadd,mailAddr,self.job.jobDefinitionID,self.job.computingSite)
                    else:
                        message = \
"""Subject: PANDA notification for JobsetID : %s  %s
From: %s
To: %s

Summary of JobsetID : %s

JobID/Site : %s""" % (jobsetID,finalStatInSub,fromadd,mailAddr,jobsetID,jobIDsite)
                    message += \
"""

Created : %s (UTC)
Ended   : %s (UTC)

Total Number of Jobs : %s
           Succeeded : %s
           Partial   : %s
           Failed    : %s
           Cancelled : %s
""" % (creationTime,endTime,nTotal,nSucceeded,nPartial,nFailed,nCancel)
                    # input datasets
                    for iDS in iDSList:
                        message += \
"""
In  : %s""" % iDS
                    # output datasets
                    for oDS in oDSList:
                        message += \
"""
Out : %s""" % oDS
                    # command
                    if self.job.metadata not in ['','NULL',None]:
                        message += \
"""

Parameters : %s""" % self.job.metadata
                    # URLs to PandaMon
                    if self.job.jobsetID in [0,'NULL',None]:
                        for tmpIdx,tmpJobID in enumerate(jobDefIDList):
                            urlData = {}
                            urlData['job'] = '*'
                            urlData['jobDefinitionID'] = tmpJobID
                            urlData['user'] = self.job.prodUserName
                            urlData['at'] = (str(creationTime)).split()[0]
                            if tmpIdx == 0:
                                message += \
"""

PandaMonURL : http://panda.cern.ch/server/pandamon/query?%s""" % urllib.urlencode(urlData)
                            else:
                                message += \
"""
              http://panda.cern.ch/server/pandamon/query?%s""" % urllib.urlencode(urlData)
                    else:
                        urlData = {}
                        urlData['job'] = '*'
                        urlData['jobsetID'] = self.job.jobsetID
                        urlData['user'] = self.job.prodUserName
                        urlData['at'] = (str(creationTime)).split()[0]
                        newUrlData = {}
                        newUrlData['jobtype'] = 'analysis'
                        newUrlData['jobsetID'] = self.job.jobsetID
                        newUrlData['prodUserName'] = self.job.prodUserName
                        newUrlData['hours'] = 71
                        message += \
"""

PandaMonURL : http://panda.cern.ch/server/pandamon/query?%s""" % urllib.urlencode(urlData)
                        if logDS is not None:
                            message += \
"""
TaskMonitorURL : https://dashb-atlas-task.cern.ch/templates/task-analysis/#task=%s""" % logDS
                        message += \
"""
NewPandaMonURL : https://pandamon.cern.ch/jobinfo?%s""" % urllib.urlencode(newUrlData)

                    # tailer
                    message += \
"""


Report Panda problems of any sort to

  the eGroup for help request
    hn-atlas-dist-analysis-help@cern.ch

  the Panda JIRA for software bug
    https://its.cern.ch/jira/browse/ATLASPANDA
"""

                    # send mail
                    self.sendMail(self.job.PandaID,fromadd,mailAddr,message,1,True)
            except Exception:
                errType,errValue = sys.exc_info()[:2]
                _logger.error("%s %s %s" % (self.job.PandaID,errType,errValue))
            _logger.debug("%s end" % self.job.PandaID)
        else:
            try:
                _logger.debug("start recovery for %s" % self.mailFileName)
                # read from file
                pandaID  = self.mailFile.readline()[:-1]
                fromadd  = self.mailFile.readline()[:-1]
                mailAddr = self.mailFile.readline()[:-1]
                message  = self.mailFile.read()
                _logger.debug("%s start recovery" % pandaID)
                if message != '':
                    self.sendMail(pandaID,fromadd,mailAddr,message,5,False)
            except Exception:
                errType,errValue = sys.exc_info()[:2]
                _logger.error("%s %s %s" % (self.mailFileName,errType,errValue))
            _logger.debug("end recovery for %s" % self.mailFileName)


    # send mail
    def sendMail(self,pandaID,fromadd,mailAddr,message,nTry,fileBackUp):
        _logger.debug("%s send to %s\n%s" % (pandaID,mailAddr,message))
        for iTry in range(nTry):
            try:
                org_smtpstderr = smtplib.stderr
                smtplib.stderr = StderrLogger(pandaID)
                smtpPort = smtpPortList[iTry % len(smtpPortList)]
                server = smtplib.SMTP(panda_config.emailSMTPsrv,smtpPort)
                server.set_debuglevel(1)
                server.ehlo()
                server.starttls()
                #server.login(panda_config.emailLogin,panda_config.emailPass)
                out = server.sendmail(fromadd,mailAddr,message)
                _logger.debug('%s %s' % (pandaID,str(out)))
                server.quit()
                break
            except Exception:
                errType,errValue = sys.exc_info()[:2]
                if iTry+1 < nTry:
                    # sleep for retry
                    _logger.debug("%s sleep %s due to %s %s" % (pandaID,iTry,errType,errValue))
                    time.sleep(30)
                else:
                    _logger.error("%s %s %s" % (pandaID,errType,errValue))
                    if fileBackUp:
                        # write to file which is processed in add.py
                        mailFile = '%s/mail_%s_%s' % (panda_config.logdir,self.job.PandaID,str(uuid.uuid4()))
                        oMail = open(mailFile,"w")
                        oMail.write(str(self.job.PandaID)+'\n'+fromadd+'\n'+mailAddr+'\n'+message)
                        oMail.close()
        try:
            smtplib.stderr = org_smtpstderr
        except Exception:
            pass



    # get email
    def getEmail(self, dn):
        # get DN
        _logger.debug("getDN for %s" % dn)
        dbProxy = DBProxy()
        distinguishedName = dbProxy.cleanUserID(dn)
        _logger.debug("DN = %s" % distinguishedName)
        if distinguishedName == "":
            _logger.error("cannot get DN for %s" % dn)
            return ""
        # get email from MetaDB
        mailAddrInDB,dbUptime = self.taskBuffer.getEmailAddr(distinguishedName,withUpTime=True)
        _logger.debug("email in MetaDB : '%s'" % mailAddrInDB)
        notSendMail = False
        if mailAddrInDB not in [None,'']:
            # email mortification is suppressed
            if mailAddrInDB.split(':')[0] == 'notsend':
                notSendMail = True
        # avoid too frequently lookup
        if dbUptime is not None and datetime.datetime.utcnow()-dbUptime < datetime.timedelta(hours=1):
            _logger.debug("no lookup")
            if notSendMail or mailAddrInDB in [None,'']:
                return 'notsend'
            else:
                return mailAddrInDB.split(':')[-1]
        # get email from DQ2
        try:
            tmpStatus,userInfo = rucioAPI.finger(dn)
            if tmpStatus:
                mailAddr = userInfo['email']
                _logger.debug("email from DDM : '%s'" % mailAddr)
            else:
                mailAddr = None
                _logger.error("failed to get email from DDM : {}".format(userInfo))
            if mailAddr is None:
                mailAddr = ''
            # make email field to update DB
            mailAddrToDB = ''
            if notSendMail:
                mailAddrToDB += 'notsend:'
            mailAddrToDB += mailAddr
            # update database
            _logger.debug("update email for %s to %s" % (distinguishedName,mailAddrToDB))
            self.taskBuffer.setEmailAddr(distinguishedName,mailAddrToDB)
            if notSendMail:
                return 'notsend'
            return mailAddr
        except Exception as e:
            _logger.error("getEmail failed with {} {}".format(str(e), traceback.format_exc()))
        return ""
