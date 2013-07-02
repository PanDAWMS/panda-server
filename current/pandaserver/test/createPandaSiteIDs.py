import re
from jobscheduler import siteinfo

from taskbuffer.DBProxy import DBProxy

# password
from config import panda_config
passwd = panda_config.dbpasswd

proxyN = DBProxy()
proxyN.connect(panda_config.logdbhost,panda_config.logdbpasswd,panda_config.logdbuser,'PandaMetaDB')

status,res = proxyN.querySQLS("SELECT nickname from schedconfig")

nicknames = []
for (nickname,) in res:
    nicknames.append(nickname)


print "PandaSiteIDs = {"
sites = siteinfo.sites.keys()
sites.sort()
for site in sites:
    vals = siteinfo.sites[site]
    okFlag = vals[10]
    fName = ''
    sitePat = site
    sitePat = re.sub('_PAUL','',sitePat)
    sitePat = re.sub('_TEST$','',sitePat)
    sitePat = re.sub('_test$','',sitePat)    
    sitePat = re.sub('^ANALY_LONG_','',sitePat)        
    sitePat = re.sub('^ANALY_','',sitePat)
    if site == 'SLACXRD':
        sitePat = 'slac'
    if site == 'UVIC':
        sitePat = 'VICTORIA'
    if sitePat == 'LYON':
        sitePat = 'IN2P3-CC-T2'
    if sitePat == 'Purdue-ITB':
        sitePat = 'Purdue'
    if sitePat == "BNL":
        sitePat = "BNL_ATLAS"
    if sitePat == "RAL":
        sitePat = "RAL-LCG2"
    if sitePat == "SACLAY":        
        sitePat = "GRIF-DAPNIA"
    for nickname in nicknames:
        if re.search(sitePat,nickname,re.I) != None:
            fName = nickname
    if fName == '':
        #print site, sitePat
        fName = 'BNL_ATLAS_1-condor'
    print "    %-22s : {'nickname':'%s','status':'%s'}," % ("'"+site+"'",fName,okFlag)
print "}"
