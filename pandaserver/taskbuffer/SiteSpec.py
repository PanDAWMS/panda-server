"""
site specification

"""

import re
from . import JobUtils


class SiteSpec(object):
    # attributes
    _attributes = ('sitename','nickname','dq2url','cloud','ddm','ddm_input','ddm_output','lfchost','type','gatekeeper',
                   'releases','memory','maxtime','status','space','retry','cmtconfig','setokens_input','setokens_output',
                   'glexec','priorityoffset','allowedgroups','defaulttoken','queue',
                   'localqueue','validatedreleases','accesscontrol','copysetup','maxinputsize',
                   'cachedse','allowdirectaccess','comment','cloudlist','statusmodtime','lfcregister',
                   'countryGroup','availableCPU','pledgedCPU','coreCount','reliabilityLevel',
                   'iscvmfs','transferringlimit','maxwdir','fairsharePolicy','minmemory','maxmemory',
                   'mintime','allowfax','wansourcelimit','wansinklimit','pandasite',
                   'sitershare','cloudrshare','corepower','wnconnectivity','catchall',
                   'role','pandasite_state','ddm_endpoints_input','ddm_endpoints_output','maxrss','minrss',
                   'direct_access_lan','direct_access_wan','tier','objectstores','is_unified',
                   'unified_name','jobseed','capability')

    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            setattr(self,attr,None)

    # serialize
    def __str__(self):
        str = ''
        for attr in self._attributes:
            str += '%s:%s ' % (attr,getattr(self,attr))
        return str


    # check if direct IO
    def isDirectIO(self):
        try:
            params = self.copysetup.split('^')
            # long format
            if len(params) >= 5: 
                # directIn
                directIn = params[4]
                if directIn == 'True':
                    return True
            # TURL PFC creation
            if len(params) == 3:
                directIn = params[2]
                if directIn == 'True':
                    return True
            if self.direct_access_lan:
                return True
        except:
            pass
        return False


    # get resource type
    def getResourceType(self):
        if self.type == 'analysis':
            return "ANALY"
        if self.minmemory > 0:
            return "HIMEM"
        if self.coreCount > 1:
            return "MCORE"
        return "SCORE"
                       


    # check if resource fair share is used
    def useResourceFairShare(self):
        if self.cloudrshare == None and self.sitershare == None:
            return False
        return True



    # check if resource fair share is used at site level
    def useSiteResourceFairShare(self):
        if self.sitershare == None:
            return False
        return True



    # check what type of jobs are allowed
    def getJobSeed(self):
        tmpVal = self.jobseed
        if tmpVal == None:
            return 'std'
        return tmpVal



    # get value from catchall
    def getValueFromCatchall(self,key):
        if self.catchall == None:
            return None
        for tmpItem in self.catchall.split(','):
            tmpMatch = re.search('^{0}=(.+)'.format(key),tmpItem)
            if tmpMatch != None:
                return tmpMatch.group(1)
        return None



    # has value in catchall
    def hasValueInCatchall(self,key):
        if self.catchall == None:
            return False
        for tmpItem in self.catchall.split(','):
            tmpMatch = re.search('^{0}(=|)*'.format(key),tmpItem)
            if tmpMatch != None:
                return True
        return False



    # allow WAN input access
    def allowWanInputAccess(self):
        return self.direct_access_lan == True and self.direct_access_wan == True



    # use jumbo jobs
    def useJumboJobs(self):
        return self.hasValueInCatchall('useJumboJobs')
        


    # get unified name
    def get_unified_name(self):
        if self.unified_name is None:
            return self.sitename
        return self.unified_name



    # get number of simulated events for dynamic number of events
    def get_n_sim_events(self):
        tmpVal = self.getValueFromCatchall('nSimEvents')
        if tmpVal is None:
            return None
        return int(tmpVal)



    # check if opportunistic
    def is_opportunistic(self):
        return self.pledgedCPU == -1



    # get number of jobs for standby
    def getNumStandby(self, sw_id, resource_type):
        # only if in standby
        if self.status not in ['standby', 'online']:
            return None
        numMap = JobUtils.parseNumStandby(self.catchall)
        # neither gshare or workqueue is definied
        if sw_id not in numMap:
            if '' in numMap:
                sw_id = ''
            else:
                return None
        # give the total if resource type is undefined
        if resource_type is None:
            return sum(numMap[sw_id].values())
        # give the number for the resource type
        if resource_type in numMap[sw_id]:
            return numMap[sw_id][resource_type]
        elif '' in numMap[sw_id]:
            return numMap[sw_id]['']
        return None
