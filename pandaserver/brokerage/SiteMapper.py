import re
import sys
import copy
import traceback
from pandaserver.config import panda_config

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('SiteMapper')

# default site
from pandaserver.taskbuffer.SiteSpec import SiteSpec
from pandaserver.taskbuffer.NucleusSpec import NucleusSpec

defSite = SiteSpec()
defSite.sitename   = panda_config.def_sitename
defSite.nickname   = panda_config.def_nickname
defSite.dq2url     = panda_config.def_dq2url
defSite.ddm_input  = panda_config.def_ddm
defSite.ddm_output = panda_config.def_ddm
defSite.type       = panda_config.def_type
defSite.gatekeeper = panda_config.def_gatekeeper
defSite.status     = panda_config.def_status
defSite.setokens_input   = {}
defSite.setokens_output   = {}

worldCloudName = 'WORLD'
nucleusTag = 'nucleus:'


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
            
            # spec for WORLD cloud
            self.worldCloudSpec = {}

            # nuclei
            self.nuclei = {}

            # satellites
            self.satellites = {}

            # get resource types
            resourceTypes = taskBuffer.load_resource_types()
            # create CloudSpec list 
            tmpCloudListDB = taskBuffer.getCloudList()
            for tmpName in tmpCloudListDB:
                tmpCloudSpec = tmpCloudListDB[tmpName]
                cloudSpec = {}
                # copy attributes from CloudSepc
                for tmpAttr in tmpCloudSpec._attributes:
                    cloudSpec[tmpAttr] = getattr(tmpCloudSpec,tmpAttr)
                # append additional attributes
                #    source : Panda siteID for source
                #    dest   : Panda siteID for dest
                #    sites  : Panda siteIDs in the cloud
                cloudSpec['source'] = cloudSpec['tier1']
                cloudSpec['dest']   = cloudSpec['tier1']
                cloudSpec['sites']  = []
                if tmpName == worldCloudName:
                    self.worldCloudSpec = cloudSpec
                else:
                    self.cloudSpec[tmpName] = cloudSpec
                    _logger.debug('Cloud->%s %s' % (tmpName,str(self.cloudSpec[tmpName])))
            # add WORLD cloud
            self.worldCloudSpec['sites']  = []
            # get list of PandaIDs
            siteIDsList = taskBuffer.getSiteList()
            firstDefault = True
            # read full list from DB
            siteFullList = taskBuffer.getSiteInfo()
            # read DB to produce paramters in siteinfo dynamically
            for tmpID in siteIDsList:
                tmpNicknameList = siteIDsList[tmpID]
                for tmpNickname in tmpNicknameList:
                    # invalid nickname
                    if tmpNickname not in siteFullList:
                        continue
                    # get full spec
                    ret = siteFullList[tmpNickname]
                    # append
                    if ret is None:
                        _logger.error('Could not read site info for %s:%s' % (tmpID,tmpNickname))
                    elif (firstDefault and tmpID == defSite.sitename) or (tmpID not in self.siteSpecList) \
                             or (tmpID in self.siteSpecList and self.siteSpecList[tmpID].status in ['offline','']):
                        # overwrite default or remove existing offline 
                        if firstDefault and tmpID == defSite.sitename:
                            del self.siteSpecList[tmpID]
                            firstDefault = False
                        elif tmpID in self.siteSpecList and self.siteSpecList[tmpID].status in ['offline','']:
                            del self.siteSpecList[tmpID]
                        # append
                        if tmpID not in self.siteSpecList:
                            # determine type following a convention
                            tmpType = 'production'
                            if tmpID.startswith('ANALY_'):
                                tmpType = 'analysis'
                            elif re.search('test',tmpID,re.I):
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
                            except Exception:
                                errtype, errvalue = sys.exc_info()[:2]
                                _logger.error("%s memory/inputsize failure : %s %s" % (tmpID,errtype,errvalue))
                    # collect nuclei and satellites
                    self.collectNS(ret) 
            # make virtual queues from unified queues
            try:
                for siteName in list(self.siteSpecList):
                    siteSpec = self.siteSpecList[siteName]
                    if siteSpec.hasValueInCatchall('unifiedPandaQueue') or siteSpec.capability == 'ucore':
                        for resourceSpec in resourceTypes:
                            # make site spec for child
                            childSiteSpec = copy.copy(siteSpec)
                            childSiteSpec.sitename = '{0}/{1}'.format(siteSpec.sitename,resourceSpec.resource_name)
                            coreCount = max(1,siteSpec.coreCount)
                            # skip if not good for core count requirement
                            if resourceSpec.mincore is not None and coreCount < resourceSpec.mincore:
                                continue
                            # change resource requirements
                            if resourceSpec.maxcore is None:
                                childSiteSpec.coreCount = max(coreCount,resourceSpec.mincore)
                            else:
                                childSiteSpec.coreCount = max(min(coreCount,resourceSpec.maxcore),resourceSpec.mincore)
                            if resourceSpec.minrampercore is not None:
                                childSiteSpec.minrss = max(childSiteSpec.coreCount*resourceSpec.minrampercore,
                                                           siteSpec.minrss)
                            if resourceSpec.maxrampercore is not None:
                                childSiteSpec.maxrss = min(childSiteSpec.coreCount*resourceSpec.maxrampercore,
                                                           siteSpec.maxrss)
                            else:
                                childSiteSpec.maxrss = min(siteSpec.maxrss * childSiteSpec.coreCount / coreCount,
                                                           siteSpec.maxrss)
                            # set unified name
                            childSiteSpec.unified_name = siteSpec.sitename
                            # append
                            self.siteSpecList[childSiteSpec.sitename] = childSiteSpec
                            self.collectNS(childSiteSpec)
                        # set unified flag
                        siteSpec.is_unified = True
            except Exception:
                _logger.error(traceback.format_exc())
            # make cloudSpec
            for siteSpec in self.siteSpecList.values():
                # choose only prod sites
                if siteSpec.type != 'production':
                    continue
                # append prod site in cloud
                for tmpCloud in siteSpec.cloudlist:
                    if tmpCloud in self.cloudSpec:
                        if not siteSpec.sitename in self.cloudSpec[tmpCloud]['sites']:
                            # append
                            self.cloudSpec[tmpCloud]['sites'].append(siteSpec.sitename)
                    else:
                        # append to the default cloud
                        if not siteSpec.sitename in self.defCloudSites:
                            # append
                            self.defCloudSites.append(siteSpec.sitename)
                # add to WORLD cloud
                if not siteSpec.sitename in self.worldCloudSpec['sites']:
                    self.worldCloudSpec['sites'].append(siteSpec.sitename)
            # set defCloudSites for backward compatibility
            if 'US' in self.cloudSpec:
                # use US sites
                self.defCloudSites = self.cloudSpec['US']['sites']
            else:
                # add def site as a protection if defCloudSites is empty
                self.defCloudSites.append(defSite.sitename)
            # dump sites
            if verbose:
                _logger.debug('========= dump =========')
                for tmpSite in self.siteSpecList:
                    tmpSiteSpec = self.siteSpecList[tmpSite]
                    _logger.debug('Site->%s' % str(tmpSiteSpec))
            # check
            for tmpCloud in self.cloudSpec:
                tmpVals = self.cloudSpec[tmpCloud]
                # set T1
                try:
                    tmpVals['sites'].remove(tmpVals['dest'])
                except Exception:
                    pass
                tmpVals['sites'].insert(0,tmpVals['dest'])
                # dump
                _logger.debug('Cloud:%s has %s' % (tmpCloud,tmpVals['sites']))
                for tmpSite in tmpVals['sites']:
                    if tmpSite not in self.siteSpecList:
                        _logger.debug("  '%s' doesn't exist" % tmpSite)
                        continue
                    tmpSiteSpec = self.siteSpecList[tmpSite]
                    if tmpSiteSpec.status in ['offline']:
                        _logger.debug('  %s:%s' % (tmpSite,tmpSiteSpec.status))
            _logger.debug('Cloud:XX has %s' % self.defCloudSites)
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error("__init__ SiteMapper : %s %s" % (type,value))
            _logger.error(traceback.format_exc())
        _logger.debug('__init__ SiteMapper done')
        

    # collect nuclei and satellites
    def collectNS(self, ret):
        # collect nuclei
        if ret.role == 'nucleus' and ret.type == 'production':
            if not ret.pandasite in self.nuclei:
                nucleus = NucleusSpec(ret.pandasite)
                nucleus.state = ret.pandasite_state
                self.nuclei[ret.pandasite] = nucleus
            self.nuclei[ret.pandasite].add(ret.sitename, ret.ddm_endpoints_output)
        # collect satellites
        if ret.role == 'satellite' and ret.type == 'production':
            if not ret.pandasite in self.satellites:
                satellite = NucleusSpec(ret.pandasite)
                satellite.state = ret.pandasite_state
                self.satellites[ret.pandasite] = satellite
            self.satellites[ret.pandasite].add(ret.sitename, ret.ddm_endpoints_output)


    # accessor for site
    def getSite(self,site):
        try:
            if site.startswith(nucleusTag):
                tmpName = site.split(':')[-1]
                if tmpName in self.nuclei:
                    site = self.nuclei[tmpName].getOnePandaSite()
                elif tmpName in self.satellites:
                    site = self.satellites[tmpName].getOnePandaSite()
        except Exception:
            pass
        if site in self.siteSpecList:
            return self.siteSpecList[site]
        else:
            # return default site
            return defSite


    # check if site exists
    def checkSite(self,site):
        try:
            if site.startswith(nucleusTag):
                tmpName = site.split(':')[-1]
                if tmpName in self.nuclei:
                    site = self.nuclei[tmpName].getOnePandaSite()
                elif tmpName in self.satellites:
                    site = self.satellites[tmpName].getOnePandaSite()
        except Exception:
            pass
        return site in self.siteSpecList


    # resolve nucleus
    def resolveNucleus(self,site):
        try:
            if site.startswith(nucleusTag):
                tmpName = site.split(':')[-1]
                if tmpName in self.nuclei:
                    site = self.nuclei[tmpName].getOnePandaSite()
                elif tmpName in self.satellites:
                    site = self.satellites[tmpName].getOnePandaSite()
        except Exception:
            pass
        if site == 'NULL':
            site = None
        return site


    # accessor for cloud
    def getCloud(self,cloud):
        if cloud in self.cloudSpec:
            return self.cloudSpec[cloud]
        elif cloud == worldCloudName:
            return self.worldCloudSpec
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
        if cloud in self.cloudSpec:
            return True
        elif cloud == worldCloudName:
            return True
        else:
            return False

        
    # accessor for cloud list
    def getCloudList(self):
        return list(self.cloudSpec)



    # get ddm point
    def getDdmEndpoint(self,siteID,storageToken):
        if not self.checkSite(siteID):
            return None
        siteSpec =  self.getSite(siteID)
        if storageToken in siteSpec.setokens_output:
            return siteSpec.setokens_output[storageToken]
        return siteSpec.ddm_output # TODO: confirm with Tadashi



    # get nucleus
    def getNucleus(self,tmpName):
        if tmpName in self.nuclei:
            return self.nuclei[tmpName]
        if tmpName in self.satellites:
            return self.satellites[tmpName]
        return None


    # get nucleus with ddm endpoint
    def getNucleusWithDdmEndpoint(self, ddmEndpoint):
        for nucleusName in self.nuclei:
            nucleusSpec = self.nuclei[nucleusName]
            if nucleusSpec.isAssociatedEndpoint(ddmEndpoint):
                return nucleusName
        return None
