import sys
import time
import datetime
import optparse

from taskbuffer.OraDBProxy import DBProxy
# password
from config import panda_config

optP = optparse.OptionParser(conflict_handler="resolve")
optP.add_option('--user', action='store',dest='user', default=None,help='prodUserName')
optP.add_option('--unban',action='store_const',const=True,dest='unban',default=False,help='unban the user')

options,args = optP.parse_args()

if options.user == None:
    print "--user=<prodUserName> is required"
    sys.exit(1)

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

prodUserName = sys.argv[1]
import userinterface.Client as Client

varMap = {}
varMap[':name'] = options.user
if options.unban:
    varMap[':status'] = None
else:
    varMap[':status'] = 'disabled'
    
sql = "UPDATE ATLAS_PANDAMETA.users SET status=:status WHERE name=:name"

status,res = proxyS.querySQLS(sql,varMap)
if res == None:
    print "Failed with database error"
else:
    print "%s rows updated" % res
    

