import os
import time
import datetime
import commands
import jobscheduler.Site
import userinterface.Client as Client
from dataservice.DDM import ddm
from taskbuffer.DBProxy import DBProxy
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger
from jobdispatcher.Watcher import Watcher

# logger
_logger = PandaLogger().getLogger('closeDS')

# password
from config import panda_config
passwd = panda_config.dbpasswd

# instantiate DB proxies
proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

# time limit for dataset closing
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=7)

# close datasets
while True:
    sql = "SELECT vuid,name,modificationdate FROM Datasets " + \
          "WHERE type='output' AND (status='running' OR status='created' OR status='defined') " + \
          "AND modificationdate<'%s' AND name REGEXP '_sub[[:digit:]]+$'"
    ret,res = proxyS.querySQLS(sql % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
    _logger.debug("# of dataset : %s" % len(res))
    if len(res) == 0:
        break
    for (vuid,name,modDate) in res:
        _logger.debug("start %s %s" % (modDate,name))
        retF,resF = proxyS.querySQLS("SELECT lfn FROM filesTable4 WHERE destinationDBlock='%s'" % name)
        if retF<0 or retF == None or retF!=len(resF):
            _logger.error("SQL error")
        else:
            # no files in filesTable
            if len(resF) == 0:
                _logger.debug("freeze %s " % name)
                status,out = ddm.dq2.main(['freezeDataset',name])
                if status != 0 or (out.find('Error') != -1 and out.find('DQ2 unknown dataset exception') == -1 \
                                   and out.find('DQ2 security exception') == -1):
                    _logger.error(out)
                else:
                    proxyS.querySQL("UPDATE Datasets SET status='completed',modificationdate=UTC_TIMESTAMP() WHERE vuid='%s'" % vuid)
            else:
                _logger.debug("wait %s " % name)
                proxyS.querySQL("UPDATE Datasets SET modificationdate=UTC_TIMESTAMP() WHERE vuid='%s'" % vuid)                
        _logger.debug("end %s " % name)
        time.sleep(1)
