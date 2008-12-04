"""
utility service

"""
import os
import jobdispatcher.Protocol as Protocol
from config import panda_config


# check if server is alive
def isAlive(req):
    return "alive=yes"


# upload file 
def putFile(req,file):
    if not Protocol.isSecure(req):
        return False
    fo = open('%s/%s' % (panda_config.cache_dir,file.filename),'wb')
    fo.write(file.file.read())
    fo.close()
    return True


# delete file 
def deleteFile(req,file):
    if not Protocol.isSecure(req):
        return 'False'
    try:
        os.remove('%s/%s' % (panda_config.cache_dir,file))
        return 'True'
    except:
        return 'False'        
                        

# get server name:port for SSL
def getServer(req):
    return "%s:%s" % (panda_config.pserverhost,panda_config.pserverport)
