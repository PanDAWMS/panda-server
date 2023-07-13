import sys

from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.brokerage import SiteMapper
from pandaserver.config import panda_config

requester_id = "{0}({1})".format(sys.modules[__name__], GenericThread().get_pid())
taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1, requester=requester_id)

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
