import re
import sys
from config import panda_config

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('SiteMapper')

# PandaIDs
from PandaSiteIDs import PandaSiteIDs

# default site
from taskbuffer.SiteSpec import SiteSpec
defSite = SiteSpec()
defSite.sitename   = panda_config.def_sitename
defSite.nickname   = panda_config.def_nickname
defSite.dq2url     = panda_config.def_dq2url
defSite.ddm        = panda_config.def_ddm
defSite.type       = panda_config.def_type
defSite.gatekeeper = panda_config.def_gatekeeper
defSite.status     = panda_config.def_status
defSite.setokens   = {}


########################################################################

class SiteMapper:
    
    # constructor
    def __init__(self,taskBuffer,verbose=False):
        _logger.debug('__init__ SiteMapper')
        try:
            # site list
            self.siteSpecList = {}

            # sites not belonging to a cloud
            self.defCloudSites = []

            # cloud specification
            self.cloudSpec = {}
            
            # create CloudSpec list 
            tmpCloudListDB = taskBuffer.getCloudList()
            for tmpName,tmpCloudSpec in tmpCloudListDB.iteritems():
                self.cloudSpec[tmpName] = {}
                # copy attributes from CloudSepc
                for tmpAttr in tmpCloudSpec._attributes:
                    self.cloudSpec[tmpName][tmpAttr] = getattr(tmpCloudSpec,tmpAttr)
                # append additional attributes
                #    source : Panda siteID for source
                #    dest   : Panda siteID for dest
                #    sites  : Panda siteIDs in the cloud
                self.cloudSpec[tmpName]['source'] = self.cloudSpec[tmpName]['tier1']
                self.cloudSpec[tmpName]['dest']   = self.cloudSpec[tmpName]['tier1']
                self.cloudSpec[tmpName]['sites']  = []
                _logger.debug('Cloud->%s %s' % (tmpName,str(self.cloudSpec[tmpName])))
            # get list of PandaIDs
            siteIDsList = taskBuffer.getSiteList()
            firstDefault = True
            # read full list from DB
            siteFullList = taskBuffer.getSiteInfo()
            # read DB to produce paramters in siteinfo dynamically
            for tmpID,tmpNicknameList in siteIDsList.iteritems():
                for tmpNickname in tmpNicknameList:
                    # invalid nickname
                    if not siteFullList.has_key(tmpNickname):
                        continue
                    # get full spec
                    ret = siteFullList[tmpNickname]
                    # append
                    if ret == None:
                        _logger.error('Could not read site info for %s:%s' % (tmpID,tmpNickname))
                    elif (firstDefault and tmpID == defSite.sitename) or (not self.siteSpecList.has_key(tmpID)) \
                             or (self.siteSpecList.has_key(tmpID) and self.siteSpecList[tmpID].status in ['offline','']):
                        # overwrite default or remove existing offline 
                        if firstDefault and tmpID == defSite.sitename:
                            del self.siteSpecList[tmpID]
                            firstDefault = False
                        elif self.siteSpecList.has_key(tmpID) and self.siteSpecList[tmpID].status in ['offline','']:
                            del self.siteSpecList[tmpID]
                        # append
                        if not self.siteSpecList.has_key(tmpID):
                            # determine type following a convention
                            tmpType = 'production'
                            if tmpID.startswith('ANALY_'):
                                tmpType = 'analysis'
                            elif re.search('test',tmpID,re.I) or \
                                     (PandaSiteIDs.has_key(tmpID) and PandaSiteIDs[tmpID]['status']!='OK'):
                                tmpType = 'test'
                            # set type
                            ret.sitename = tmpID
                            ret.type     = tmpType
                            # don't use site for production when cloud is undefined
                            if ret.type == 'production' and ret.cloud == '':
                                _logger.error('Empty cloud for %s:%s' % (tmpID,tmpNickname))
                            else:
                                self.siteSpecList[tmpID] = ret
                    else:
                        # overwrite status
                        if not ret.status in ['offline','']:
                            if self.siteSpecList[tmpID].status != 'online':
                                self.siteSpecList[tmpID].status = ret.status
                            # use larger maxinputsize and memory
                            try:
                                if ret.status in ['online']:
                                    if self.siteSpecList[tmpID].maxinputsize < ret.maxinputsize or \
                                           ret.maxinputsize == 0:
                                        self.siteSpecList[tmpID].maxinputsize = ret.maxinputsize
                                    if (self.siteSpecList[tmpID].memory != 0 and self.siteSpecList[tmpID].memory < ret.memory) or \
                                           ret.memory == 0:
                                        self.siteSpecList[tmpID].memory = ret.memory
                            except:
                                errtype, errvalue = sys.exc_info()[:2]
                                _logger.error("%s memory/inputsize failuer : %s %s" % (tmpID,errtype,errvalue))
            # make cloudSpec
            for siteSpec in self.siteSpecList.values():
                # choose only prod sites
                if siteSpec.type != 'production':
                    continue
                # append prod site in cloud
                for tmpCloud in siteSpec.cloudlist:
                    if self.cloudSpec.has_key(tmpCloud):
                        if not siteSpec.sitename in self.cloudSpec[tmpCloud]['sites']:
                            # append
                            self.cloudSpec[tmpCloud]['sites'].append(siteSpec.sitename)
                    else:
                        # append to the default cloud
                        if not siteSpec.sitename in self.defCloudSites:
                            # append
                            self.defCloudSites.append(siteSpec.sitename)
            # set defCloudSites for backward compatibility
            if self.cloudSpec.has_key('US'):
                # use US sites
                self.defCloudSites = self.cloudSpec['US']['sites']
            else:
                # add def site as a protection if defCloudSites is empty
                self.defCloudSites.append(defSite.sitename)
            # dump sites
            if verbose:
                _logger.debug('========= dump =========')
                for tmpSite,tmpSiteSpec in self.siteSpecList.iteritems():
                    _logger.debug('Site->%s' % str(tmpSiteSpec))
            # check
            for tmpCloud,tmpVals in self.cloudSpec.iteritems():
                # set T1
                try:
                    tmpVals['sites'].remove(tmpVals['dest'])
                except:
                    pass
                tmpVals['sites'].insert(0,tmpVals['dest'])
                # dump
                _logger.debug('Cloud:%s has %s' % (tmpCloud,tmpVals['sites']))
                for tmpSite in tmpVals['sites']:
                    if not self.siteSpecList.has_key(tmpSite):
                        _logger.debug("  '%s' doesn't exist" % tmpSite)
                        continue
                    tmpSiteSpec = self.siteSpecList[tmpSite]
                    if tmpSiteSpec.status in ['offline']:
                        _logger.debug('  %s:%s' % (tmpSite,tmpSiteSpec.status))
            _logger.debug('Cloud:XX has %s' % self.defCloudSites)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("__init__ SiteMapper : %s %s" % (type,value))
        _logger.debug('__init__ SiteMapper done')
        

    # accessor for site
    def getSite(self,site):
        if self.siteSpecList.has_key(site):
            return self.siteSpecList[site]
        else:
            # return default site
            return defSite


    # check if site exists
    def checkSite(self,site):
        return self.siteSpecList.has_key(site)


    # accessor for cloud
    def getCloud(self,cloud):
        if self.cloudSpec.has_key(cloud):
            return self.cloudSpec[cloud]
        else:
            # return sites in default cloud
            ret = { 'source'      : 'default',
                    'dest'        : 'default',
                    'sites'       : self.defCloudSites,
                    'transtimelo' : 2,
                    'transtimehi' : 1,
                    }
            return ret


    # accessor for cloud
    def checkCloud(self,cloud):
        if self.cloudSpec.has_key(cloud):
            return True
        else:
            return False

        
    # accessor for cloud list
    def getCloudList(self):
        return self.cloudSpec.keys()



    # get ddm point
    def getDdmEndpoint(self,siteID,storageToken):
        if not self.checkSite(siteID):
            return None
        siteSpec =  self.getSite(siteID)
        if siteSpec.setokens.has_key(storageToken):
            return siteSpec.setokens[storageToken]
        return siteSpec.ddm
