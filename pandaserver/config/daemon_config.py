import re
import sys
from typing import Any

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

# ---------------------------------------------------------------------------
# As in panda_config, the attributes of this module are installed at import time
# by expand_values() and by the default above, so a type checker sees none of
# them. Only the two read as plain attributes are declared here; every other
# daemon setting is read with getattr() and a default, which needs no
# declaration and would be misdescribed by one, since it may well be absent.
# ---------------------------------------------------------------------------

# the daemon table, either the JSON text from the cfg or an already parsed dict
# from the config map. DaemonMaster._parse_config() accepts both
config: str | dict[str, Any]

# whether the daemon master is allowed to run at all
enable: bool
