import re
import sys
import copy
import traceback
from pandaserver.config import panda_config

from pandacommon.pandalogger.PandaLogger import PandaLogger

# default site
from pandaserver.taskbuffer.SiteSpec import SiteSpec
from pandaserver.taskbuffer.NucleusSpec import NucleusSpec

from pandaserver.dataservice.DataServiceUtils import select_scope

# logger
_logger = PandaLogger().getLogger('SiteMapper')

defSite = SiteSpec()
defSite.sitename   = panda_config.def_sitename
defSite.nickname   = panda_config.def_nickname
defSite.ddm_input  = {'default': panda_config.def_ddm}
defSite.ddm_output = {'default': panda_config.def_ddm}
defSite.type       = panda_config.def_type
defSite.status     = panda_config.def_status
defSite.setokens_input   = {}
defSite.setokens_output   = {}
defSite.ddm_endpoints_input = {}
defSite.ddm_endpoints_output = {}

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
            self.worldCloudSpec['sites'] = []
            firstDefault = True
            # read full list from DB
            siteFullList = taskBuffer.getSiteInfo()
            siteIDsList = {}
            for tmpNickname in siteFullList:
                siteIDsList.setdefault(siteFullList[tmpNickname].sitename, [])
                siteIDsList[siteFullList[tmpNickname].sitename].append(tmpNickname)
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
                            # don't use site for production when cloud is undefined
                            if ret.runs_production() and ret.cloud == '':
                                _logger.error('Empty cloud for %s:%s' % (tmpID,tmpNickname))
                            else:
                                self.siteSpecList[tmpID] = ret
                    else:
                        # overwrite status
                        if ret.status not in ['offline','']:
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
                                childSiteSpec.minrss = max(childSiteSpec.coreCount * resourceSpec.minrampercore,
                                                           siteSpec.minrss * childSiteSpec.coreCount / coreCount)
                            else:
                                childSiteSpec.minrss = max(siteSpec.minrss * childSiteSpec.coreCount / coreCount,
                                                           siteSpec.minrss)
                            if resourceSpec.maxrampercore is not None:
                                childSiteSpec.maxrss = min(childSiteSpec.coreCount * resourceSpec.maxrampercore,
                                                           siteSpec.maxrss * childSiteSpec.coreCount / coreCount)
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
                # choose only prod or grandly unified sites
                if not siteSpec.runs_production():
                    continue
                # append prod site in cloud
                for tmpCloud in siteSpec.cloudlist:
                    if tmpCloud in self.cloudSpec:
                        if siteSpec.sitename not in self.cloudSpec[tmpCloud]['sites']:
                            # append
                            self.cloudSpec[tmpCloud]['sites'].append(siteSpec.sitename)
                    else:
                        # append to the default cloud
                        if siteSpec.sitename not in self.defCloudSites:
                            # append
                            self.defCloudSites.append(siteSpec.sitename)
                # add to WORLD cloud
                if siteSpec.sitename not in self.worldCloudSpec['sites']:
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
        if ret.runs_production():
            if ret.role == 'nucleus':
                target = self.nuclei
            elif ret.role == 'satellite':
                target = self.satellites
            else:
                return
            if ret.pandasite not in target:
                atom = NucleusSpec(ret.pandasite)
                atom.state = ret.pandasite_state
                mode = ret.bare_nucleus_mode()
                if mode:
                    atom.set_bare_nucleus_mode(mode)
                secondary = ret.secondary_nucleus()
                if secondary:
                    atom.set_secondary_nucleus(secondary)
                if ret.role == 'satellite':
                    atom.set_satellite()
                try:
                    atom.set_default_endpoint_out(ret.ddm_endpoints_output['default'].getDefaultWrite())
                except Exception:
                    pass
                target[ret.pandasite] = atom
            target[ret.pandasite].add(ret.sitename, ret.ddm_endpoints_output, ret.ddm_endpoints_input)

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
    def getDdmEndpoint(self, siteID, storageToken, prodSourceLabel, job_label):
        if not self.checkSite(siteID):
            return None
        siteSpec =  self.getSite(siteID)
        scope_input, scope_output = select_scope(siteSpec, prodSourceLabel, job_label)
        if storageToken in siteSpec.setokens_output[scope_output]:
            return siteSpec.setokens_output[scope_output][storageToken]
        return siteSpec.ddm_output[scope_output]



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
