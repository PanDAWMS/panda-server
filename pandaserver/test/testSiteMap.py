import sys

from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.brokerage import SiteMapper
from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import taskBuffer

requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1, requester=requester_id)

site_mapper = SiteMapper.SiteMapper(taskBuffer)

"""
x = site_mapper.getSite('ANALY_DESY-HH')
print(x.ddm_input)
print(x.ddm_output)
print(x.setokens_input)
print(x.setokens_output)

x = site_mapper.getSite('BNL_PROD')
print(x.ddm_input)
print(x.ddm_output)
print(x.setokens_input)
print(x.setokens_output)
"""
