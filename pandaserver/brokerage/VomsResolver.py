import re
import sys

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('VomsResolver')


########################################################################

class VomsResolver:
    
    # constructor
    def __init__(self):
        self.vomsUserMap = {}
        try:
            # read grid-mapfile
            mapFile = open('/home/sm/grid-mapfile')
            vo = None
            for line in mapFile:
                if line.startswith("#----"):
                    # get vo name
                    vo = line.split()[-2]
                    _logger.debug('get VO:%s' % vo)
                    self.vomsUserMap[vo] = []
                else:
                    # get DN
                    match = re.search('^"([^"]+)"',line)
                    if match != None:
                        # append
                        self.vomsUserMap[vo] = match.group(1)
            # close grid-mapfile
            mapFile.close()
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("init : %s %s" % (type,value))


    # check the user is on VO
    def checkUser(self,voms,dn):
        _logger.debug('checkUser VO:%s DN:%s' % (voms,dn))
        if not self.vomsUserMap.has_key(voms):
            _logger.debug(' NG - VO:%s is unsupported' % voms)
            return False
        # look for DN
        for tmpDN in self.vomsUserMap[voms]:
            if dn.startswith(tmpDN):
                _logger.debug(' OK' % dn)
                return True
        _logger.debug(' NG - DN:%s is not found' % dn)
        return False


    # check voms is supported
    def checkVoms(self,voms):
        return self.vomsUserMap.has_key(voms)
