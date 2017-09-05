from taskbuffer.TaskBuffer import taskBuffer
from brokerage import SiteMapper

# password
from config import panda_config
passwd = panda_config.dbpasswd

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

siteMapper = SiteMapper.SiteMapper(taskBuffer)

"""
x = siteMapper.getSite('ANALY_DESY-HH')
print x.ddm_input
print x.ddm_output
print x.setokens_input
print x.setokens_output

x = siteMapper.getSite('BNL_PROD')
print x.ddm_input
print x.ddm_output
print x.setokens_input
print x.setokens_output
"""