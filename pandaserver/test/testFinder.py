import sys
from taskbuffer.OraDBProxy import DBProxy

from dataservice import AddressFinder

# password
from config import panda_config
passwd = panda_config.dbpasswd

# instantiate DB proxies
proxyS = DBProxy(True)
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

# get DN and address
status,res = proxyS.querySQLS("SELECT dn,email,name FROM ATLAS_PANDAMETA.users",{},arraySize=1000000)
if res == None:
    print "SQL error"
    sys.exit(0)
    
for dn,origEmail,name in res:
    if dn == None:
        dn = name
    if dn == None:
        continue
    emails = AddressFinder.getEmailPhonebook(dn)
    if len(emails) == 0:
        #print dn
        #print "ERROR : not found"
        pass
    elif len(emails) > 1:
        print dn        
        print "ERROR : non-unique %s" % str(emails)
    elif origEmail == None or origEmail.upper() != emails[0].upper():
        print dn        
        print "ERROR : %-20s   new:%s\n" % (origEmail,emails[0])
        pass
    else:
        pass
        #print dn
        #print "OK"

