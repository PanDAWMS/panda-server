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

# to upper chrs
def toUpper(emails):
    retA = []
    for email in emails:
        retA.append(email.upper())
    return retA

outF = open('newemail.sql','w')

for dn,origEmail,name in res:
    if dn == None:
        dn = name
    if dn == None:
        continue
    emailsP = AddressFinder.getEmailPhonebook(dn)
    emailsX = AddressFinder.getEmailXwho(dn)
    if toUpper(emailsP) != toUpper(emailsX) and len(emailsP) != 0:
        print dn
        print "ERROR : xwho != phone"
        print "phone : %s" % str(emailsP)
        print "xwho  : %s" % str(emailsX)
        print "DB : %s" % origEmail
        print
    elif len(emailsP) == 0:
        print dn
        print "ERROR : not found"
        print "DB : %s" % origEmail
        print
    elif len(emailsP) > 1:
        print dn        
        print "ERROR : non-unique %s" % str(emailsP)
        print "DB : %s" % origEmail
        print
    elif origEmail == None or origEmail.upper() != emailsP[0].upper() and origEmail != 'notsend':
        print dn        
        print "phone : %s" % str(emailsP)
        print "xwho  : %s" % str(emailsX)
        print "ERROR : %-40s    new:   %s\n" % (origEmail,emailsP[0])
        outF.write("/* %-40s    new:   %s */\n" % (origEmail,emailsP[0]))
        outF.write("UPDATE atlas_pandameta.users SET email='%s' WHERE name='%s';\n" % (emailsP[0],name))
        pass
    else:
        pass
        #print dn
        #print "OK"

outF.write('COMMIT;')
outF.close()


