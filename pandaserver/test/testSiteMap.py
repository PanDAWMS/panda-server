from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.brokerage import SiteMapper

# password
from pandaserver.config import panda_config

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