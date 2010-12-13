"""
site specification

"""

class SiteSpec(object):
    # attributes
    _attributes = ('sitename','nickname','dq2url','cloud','ddm','lfchost','se','type','gatekeeper',
                   'releases','memory','maxtime','status','space','retry','cmtconfig','setokens',
                   'seprodpath','glexec','priorityoffset','allowedgroups','defaulttoken','queue',
                   'localqueue','validatedreleases','accesscontrol','copysetup','maxinputsize',
                   'cachedse','allowdirectaccess')

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

        

                       
