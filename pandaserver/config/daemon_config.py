import re
import sys

from pandacommon.liveconfigparser.LiveConfigParser import (
    LiveConfigParser,
    expand_values,
)

from . import config_utils

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read("panda_server.cfg")

# get daemon section
tmpDict = getattr(tmpConf, "daemon", {})

# read configmap
config_utils.load_config_map("daemon", tmpDict)

# expand all values
tmpSelf = sys.modules[__name__]
expand_values(tmpSelf, tmpDict)

# default values
if "enable" not in tmpSelf.__dict__:
    tmpSelf.__dict__["enable"] = False
