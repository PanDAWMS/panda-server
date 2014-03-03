"""
site specification

"""

class SiteSpec(object):
    # attributes
    _attributes = ('sitename','nickname','dq2url','cloud','ddm','lfchost','se','type','gatekeeper',
                   'releases','memory','maxtime','status','space','retry','cmtconfig','setokens',
                   'seprodpath','glexec','priorityoffset','allowedgroups','defaulttoken','queue',
                   'localqueue','validatedreleases','accesscontrol','copysetup','maxinputsize',
                   'cachedse','allowdirectaccess','comment','cloudlist','statusmodtime','lfcregister',
                   'countryGroup','availableCPU','pledgedCPU','coreCount','reliabilityLevel',
                   'iscvmfs','transferringlimit','maxwdir','fairsharePolicy','minmemory','maxmemory',
                   'mintime','allowfax')

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
        except:
            pass
        return False
                       
