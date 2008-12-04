'''
setup cloud

'''

import re
import sys
import time
import random
import brokerage.broker_util
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('TaskAssigner')


class TaskAssigner:
    # constructor
    def __init__(self,taskBuffer,siteMapper,taskID,prodSourceLabel):
        self.taskBuffer = taskBuffer
        self.siteMapper = siteMapper
        self.taskID     = taskID
        self.cloudTask  = None
        self.prodSourceLabel = prodSourceLabel

    # check cloud
    def checkCloud(self):
        try:
            _logger.debug('%s checkCloud' % self.taskID)
            # get CloudTask from DB
            self.cloudTask = self.taskBuffer.getCloudTask(self.taskID)
            if self.cloudTask == None:
                _logger.error('%s cannot get CloudTask' % self.taskID)
                return None
            # if already assigned
            if self.cloudTask.status == 'assigned':
                _logger.debug('%s checked Cloud -> %s' % (self.taskID,self.cloudTask.cloud))
                return self.cloudTask.cloud
            # return "" to set cloud later
            _logger.debug('%s return Cloud=""' % self.taskID)
            return ""
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("checkCloud : %s %s" % (type,value))
            return None


    # set cloud
    def setCloud(self,lfns,guids,locations={}):
        try:
            _logger.debug('%s setCloud' % self.taskID)        
            # get cloud list
            cloudList = self.siteMapper.getCloudList()
            # get pilot statistics
            nWNmap = self.taskBuffer.getCurrentSiteData()
            # remove offline clouds, or non-validation cloud if validation
            tmpCloudList = []
            for tmpCloudName in cloudList:
                # get cloud
                tmpCloud = self.siteMapper.getCloud(tmpCloudName)
                # skip offline  clouds
                if tmpCloud['status'] in ['offline']:
                    _logger.debug('%s skip %s status:%s' % (self.taskID,tmpCloudName,tmpCloud['status']))
                    continue
                # skip non-validation cloud if validation
                if self.prodSourceLabel in ['validation'] and tmpCloud['validation'] != 'true':
                    _logger.debug("%s skip %s validation:'%s'" % (self.taskID,tmpCloudName,tmpCloud['validation']))
                    continue
                # append
                tmpCloudList.append(tmpCloudName)
            cloudList = tmpCloudList
            # DQ2 location info
            _logger.debug('%s DQ2 locations %s' % (self.taskID,str(locations)))
            incompleteClouds = []
            if locations != {}:
                removedCloud = []
                for dataset,sites in locations.iteritems():
                    _logger.debug('%s DS:%s' % (self.taskID,dataset)) 
                    for tmpCloudName in cloudList:
                        tmpCloud = self.siteMapper.getCloud(tmpCloudName)
                        # look for T1 SE which holds the max number of files
                        minFound = -1
                        foundSE  = ''
                        for tmpSE in tmpCloud['tier1SE']:
                            if tmpSE in sites.keys():
                                tmpStat = sites[tmpSE][-1]
                                if minFound < tmpStat['found']:
                                    minFound = tmpStat['found']
                                    foundSE  = tmpSE
                        # remove cloud if T1SE is not a location
                        if foundSE == '':
                            _logger.debug('%s   removed %s' % (self.taskID,tmpCloudName))
                            if not tmpCloudName in removedCloud:
                                removedCloud.append(tmpCloudName)
                        else:
                            # check incomplete or not
                            tmpStat = sites[foundSE][-1]
                            if tmpStat['found'] < tmpStat['total']:
                                _logger.debug('%s   incomplete %s' % (self.taskID,tmpCloudName))
                                if not tmpCloudName in incompleteClouds:
                                    incompleteClouds.append(tmpCloudName)
                # remove clouds
                for tmpCloudName in removedCloud:
                    if tmpCloudName in cloudList:
                        cloudList.remove(tmpCloudName)
            _logger.debug('%s new locations after DQ2 filter %s' % (self.taskID,str(cloudList)))
            if cloudList == []:
                raise RuntimeError, 'cloud list is empty after DQ2 filter'
            # list of completed cloud
            completedCloud = []
            for tmpCloudName in cloudList:
                if not tmpCloudName in incompleteClouds:
                    completedCloud.append(tmpCloudName)
            _logger.debug('%s comp clouds %s' % (self.taskID,str(completedCloud)))
            # calculate # of loops
            nFile = 200
            nLoop = len(guids) / nFile
            if len(guids) % nFile != 0:
                nLoop += 1
            iFileList = []
            for iTmp in range(nLoop):
                iFileList.append(iTmp*nFile)
            # truncate list to avoid too many lookup     
            maxLoop = 100
            if len(iFileList) > maxLoop:
                random.shuffle(iFileList)
                iFileList = iFileList[:maxLoop]
                iFileList.sort()
            # loop over all cloud
            weightParams = {}
            for tmpCloudName in cloudList:
                _logger.debug('%s calculate weight for %s' % (self.taskID,tmpCloudName))
                # get cloud
                tmpCloud = self.siteMapper.getCloud(tmpCloudName)
                weightParams[tmpCloudName] = {}            
                # get T1 site
                tmpT1Site = self.siteMapper.getSite(tmpCloud['source'])
                # DQ2 URL
                dq2URL = tmpT1Site.dq2url
                dq2ID  = tmpT1Site.ddm
                # set LFC and SE name 
                tmpSE = []
                if not tmpT1Site.lfchost in [None,'']:
                    dq2URL = 'lfc://'+tmpT1Site.lfchost+':/grid/atlas/'
                    tmpSE = []
                    if tmpT1Site.se != None:
                        for tmpSrcSiteSE in tmpT1Site.se.split(','):
                            match = re.search('.+://([^:/]+):*\d*/*',tmpSrcSiteSE)
                            if match != None:
                                tmpSE.append(match.group(1))
                # get files from LRC
                weightParams[tmpCloudName]['nFiles'] = 0
                # loop over all files
                iLoop = 0
                skipFlag = False
                for iFile in iFileList:
                    nTry = 3
                    for iTry in range(nTry):
                        tmpOKFiles = brokerage.broker_util.getFilesFromLRC(lfns[iFile:iFile+nFile],dq2URL,guids=guids[iFile:iFile+nFile],
                                                                           storageName=tmpSE,terminateWhenFailed=True)
                        if tmpOKFiles == None:
                            if iTry+1 == nTry:
                                # remove from completedCloud
                                if tmpCloudName in completedCloud:
                                    completedCloud.remove(tmpCloudName)
                                # fail if no completedCloud
                                if True: # if completedCloud == []: # TEMP disabled to fail for any LFC/LRC error
                                    raise RuntimeError, 'invalid return from getFilesFromLRC'
                                else:
                                    skipFlag = True
                                    break
                            else:
                                # retry
                                time.sleep(60)
                        else:
                            break
                    weightParams[tmpCloudName]['nFiles'] += len(tmpOKFiles)
                    iLoop += 1
                    if iLoop % 10 == 1:
                        _logger.debug('%s %s %s' % (self.taskID,len(guids),iFile))
                # skip 
                if skipFlag:
                    _logger.debug('%s    %s skip : invalid retur from LFC' % (self.taskID,cloudName))
                    del weightParams[tmpCloudName]
                    continue
                _logger.debug('%s  # of files  %s' % (self.taskID,weightParams[tmpCloudName]['nFiles']))
                # get number of running jobs
                nPilot = 0
                for siteName in tmpCloud['sites']:
                    if nWNmap.has_key(siteName):
                        nPilot += (nWNmap[siteName]['getJob'] + nWNmap[siteName]['updateJob'])
                weightParams[tmpCloudName]['nPilot'] = nPilot
                _logger.debug('%s  # of pilots %s' % (self.taskID,nPilot))
                # available space
                weightParams[tmpCloudName]['space'] = tmpT1Site.space
                _logger.debug('%s  T1 space    %s' % (self.taskID,tmpT1Site.space))                
            # compare parameters
            definedCloud = "US"
            maxNFiles = 0
            maxClouds = []
            for cloudName,params in weightParams.iteritems():
                # compare # of files
                if params['nFiles'] > maxNFiles:
                    maxNFiles = params['nFiles']
                    maxClouds = [cloudName]
                elif params['nFiles'] == maxNFiles:
                    maxClouds.append(cloudName)
            # space check
            if len(maxClouds) > 1:
                tmpMaxClouds = []
                for cloudName in maxClouds:
                    # ignore non-active cloud
                    if weightParams[cloudName]['nPilot'] == 0:
                        _logger.debug('%s    %s skip : 0 pilots' % (self.taskID,cloudName))
                        continue
                    # calculate space per 1000 CPU's
                    aveSpace = weightParams[cloudName]['space'] * 1000 / weightParams[cloudName]['nPilot']
                    # no task is assigned if available space is less than 10TB per 1000 CPU's
                    if aveSpace < (10 * 1024):
                        _logger.debug('%s    %s skip : space=%s total=%s' % \
                                      (self.taskID,cloudName,aveSpace,weightParams[cloudName]['space']))
                        continue
                    else:
                        _logger.debug('%s    %s pass : space=%s total=%s' % \
                                      (self.taskID,cloudName,aveSpace,weightParams[cloudName]['space']))
                        tmpMaxClouds.append(cloudName)
                # use new maxCloud
                if tmpMaxClouds != []:
                    _logger.debug('%s    use new clouds after space checking' % self.taskID)
                    maxClouds = tmpMaxClouds
                else:
                    _logger.debug('%s    use old clouds' % self.taskID)
            # choose one
            if len(maxClouds) == 1:
                definedCloud = maxClouds[0]
            elif len(maxClouds) > 1:
                nPilotList = []
                totalRequest = 0
                for cloudName in maxClouds:
                    # choose cloud according to # of pilot requests
                    nPilot = weightParams[cloudName]['nPilot']
                    if nPilot == 0:
                        nPilot = 1
                    nPilotList.append(nPilot)
                    totalRequest += nPilot
                # determin cloud using random number
                rNumber = random.randint(1,totalRequest)
                _logger.debug('%s    rNumber  %s' % (self.taskID,rNumber))
                for index,nPilot in enumerate(nPilotList):
                    rNumber -= nPilot
                    _logger.debug('%s    rNumber  %s : Cloud=%s nPilot=%s' % 
                                  (self.taskID,rNumber,maxClouds[index],nPilot))
                    if rNumber <= 0:
                        definedCloud = maxClouds[index]
                        break
            # set CloudTask in DB
            self.cloudTask.cloud = definedCloud
            retCloudTask = self.taskBuffer.setCloudTask(self.cloudTask)
            if retCloudTask == None:
                _logger.error('%s cannot set CloudTask' % self.taskID)
                return None
            _logger.debug('%s set Cloud -> %s' % (self.taskID,retCloudTask.cloud))
            # return
            return retCloudTask.cloud
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("setCloud : %s %s" % (type,value))
            return None
            
