'''
update dataset DB, and then close dataset and start Activator if needed

'''

import re
import time
import urllib
import commands
import threading
from DDM import ddm
from Notifier import Notifier
from Activator import Activator
from pandalogger.PandaLogger import PandaLogger
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec
from taskbuffer.DatasetSpec import DatasetSpec
from brokerage.SiteMapper import SiteMapper
from config import panda_config
import brokerage.broker_util

# logger
_logger = PandaLogger().getLogger('Closer')


class Closer (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,destinationDBlocks,job,pandaDDM=False,datasetMap={}):
        threading.Thread.__init__(self)
        self.taskBuffer = taskBuffer
        self.destinationDBlocks = destinationDBlocks
        self.job = job
        self.pandaID = job.PandaID
        self.pandaDDM = pandaDDM
        self.siteMapper = None
        self.datasetMap = datasetMap


    # main
    def run(self):
        _logger.debug('%s Start %s' % (self.pandaID,self.job.jobStatus))
        flagComplete = True
        ddmJobs = []
        for destinationDBlock in self.destinationDBlocks:
            dsList = []
            _logger.debug('%s start %s' % (self.pandaID,destinationDBlock))
            # ignore tid datasets
            if re.search('_tid[\d_]+$',destinationDBlock):
                _logger.debug('%s skip %s' % (self.pandaID,destinationDBlock))                
                continue
            # query dataset
            if self.datasetMap.has_key(destinationDBlock):
                dataset = self.datasetMap[destinationDBlock]
            else:
                dataset = self.taskBuffer.queryDatasetWithMap({'name':destinationDBlock})
            if dataset == None:
                _logger.error('%s Not found : %s' % (self.pandaID,destinationDBlock))
                flagComplete = False
                continue
            dsList.append(dataset)
            # sort
            dsList.sort()
            # count number of completed files
            notFinish = self.taskBuffer.countFilesWithMap({'destinationDBlock':destinationDBlock,
                                                           'status':'unknown'})
            if notFinish < 0:
                _logger.error('%s Invalid DB return : %s' % (self.pandaID,notFinish))
                flagComplete = False                
                continue
            # check if completed
            _logger.debug('%s notFinish:%s' % (self.pandaID,notFinish))
            if self.job.destinationSE == 'local' and self.job.prodSourceLabel in ['user','panda']:
                # close non-DQ2 destinationDBlock immediately
                finalStatus = 'closed'                
            else:
                # set status to 'tobeclosed' to trigger DQ2 closing
                finalStatus = 'tobeclosed'
            if notFinish==0: 
                _logger.debug('%s close dataset : %s' % (self.pandaID,destinationDBlock))
                # set status
                dataset.status = finalStatus
                # update dataset in DB
                retT = self.taskBuffer.updateDatasets(dsList,withLock=True,withCriteria="status<>:crStatus",
                                                      criteriaMap={':crStatus':finalStatus})
                if len(retT) > 0 and retT[0]==1:
                    if self.pandaDDM and self.job.prodSourceLabel=='managed':
                        # instantiate SiteMapper
                        if self.siteMapper == None:
                            self.siteMapper = SiteMapper(self.taskBuffer)
                        # get file list for PandaDDM
                        retList = self.taskBuffer.queryFilesWithMap({'destinationDBlock':destinationDBlock})
                        lfnsStr = ''
                        guidStr = ''
                        for tmpFile in retList:
                            if tmpFile.type in ['log','output']:
                                lfnsStr += '%s,' % tmpFile.lfn
                                guidStr += '%s,' % tmpFile.GUID
                        if lfnsStr != '':
                            guidStr = guidStr[:-1]
                            lfnsStr = lfnsStr[:-1]
                            # create a DDM job
                            ddmjob = JobSpec()
                            ddmjob.jobDefinitionID   = int(time.time()) % 10000
                            ddmjob.jobName           = "%s" % commands.getoutput('uuidgen')
                            ddmjob.transformation    = 'https://gridui01.usatlas.bnl.gov:24443/dav/test/run_dq2_cr'
                            ddmjob.destinationDBlock = 'testpanda.%s' % ddmjob.jobName
                            ddmjob.computingSite     = "BNL_ATLAS_DDM"
                            ddmjob.destinationSE     = ddmjob.computingSite
                            ddmjob.currentPriority   = 200000
                            ddmjob.prodSourceLabel   = 'ddm'
                            ddmjob.transferType      = 'sub'
                            # append log file
                            fileOL = FileSpec()
                            fileOL.lfn = "%s.job.log.tgz" % ddmjob.jobName
                            fileOL.destinationDBlock = ddmjob.destinationDBlock
                            fileOL.destinationSE     = ddmjob.destinationSE
                            fileOL.dataset           = ddmjob.destinationDBlock
                            fileOL.type = 'log'
                            ddmjob.addFile(fileOL)
                            # make arguments
                            dstDQ2ID = 'BNLPANDA'
                            srcDQ2ID = self.siteMapper.getSite(self.job.computingSite).ddm
                            callBackURL = 'https://%s:%s/server/panda/datasetCompleted?vuid=%s&site=%s' % \
                                          (panda_config.pserverhost,panda_config.pserverport,
                                           dataset.vuid,dstDQ2ID)
                            _logger.debug(callBackURL)
                            # set src/dest
                            ddmjob.sourceSite      = srcDQ2ID
                            ddmjob.destinationSite = dstDQ2ID
                            # if src==dst, send callback without ddm job
                            if dstDQ2ID == srcDQ2ID:
                                comout = commands.getoutput('curl -k %s' % callBackURL)
                                _logger.debug(comout)
                            else:
                                # run dq2_cr
                                callBackURL = urllib.quote(callBackURL)
                                # get destination dir
                                destDir = brokerage.broker_util._getDefaultStorage(self.siteMapper.getSite(self.job.computingSite).dq2url)
                                argStr = "-s %s -r %s --guids %s --lfns %s --callBack %s -d %s/%s %s" % \
                                         (srcDQ2ID,dstDQ2ID,guidStr,lfnsStr,callBackURL,destDir,
                                          destinationDBlock,destinationDBlock)
                                # set job parameters
                                ddmjob.jobParameters = argStr
                                _logger.debug('%s pdq2_cr %s' % (self.pandaID,ddmjob.jobParameters))
                                ddmJobs.append(ddmjob)
                    # start Activator
                    if re.search('_sub\d+$',dataset.name) == None:
                        if self.job.jobStatus == 'finished':
                            aThr = Activator(self.taskBuffer,dataset)
                            aThr.start()
                            aThr.join()
                else:
                    # unset flag since another thread already updated 
                    flagComplete = False
            else:
                # update dataset in DB
                self.taskBuffer.updateDatasets(dsList,withLock=True,withCriteria="status<>:crStatus",
                                               criteriaMap={':crStatus':finalStatus})
                # unset flag
                flagComplete = False
            # end
            _logger.debug('%s end %s' % (self.pandaID,destinationDBlock))
        # start DDM jobs
        if ddmJobs != []:
            self.taskBuffer.storeJobs(ddmJobs,self.job.prodUserID,joinThr=True)
        # start notifier
        _logger.debug('%s source:%s complete:%s' % (self.pandaID,self.job.prodSourceLabel,flagComplete))
        if (self.job.jobStatus != 'transferring') and ((flagComplete and self.job.prodSourceLabel=='user') or \
           (self.job.jobStatus=='failed' and self.job.prodSourceLabel=='panda')):
            nThr = Notifier(self.taskBuffer,self.job,self.destinationDBlocks)
            nThr.start()
            nThr.join()            
            
        _logger.debug('%s End' % self.pandaID)
