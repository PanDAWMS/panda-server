import json
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


def get_timeout(attrs: dict[str, Any]) -> int:
    """
    Get how long one run of a daemon may take before the daemon master kills its worker.

    Args:
        attrs(dict): the entry of the daemon in config, which must have a period

    Returns:
        int: the timeout of the entry, or else three periods but at most a period and an hour
    """
    timeout: int = attrs["timeout"] if "timeout" in attrs else min(attrs["period"] * 3, attrs["period"] + 3600)
    return timeout


def find_daemon(module: str) -> dict[str, Any] | None:
    """
    Get the entry of config that runs a daemon module.

    Args:
        module(str): name of the module under the daemon package, such as async_request_daemon

    Returns:
        dict or None: the first enabled entry running the module, or None when there is none
    """
    table = getattr(tmpSelf, "config", None)
    if table is None:
        return None
    if not isinstance(table, dict):
        table = json.loads(table)
    for name, attrs in table.items():
        if attrs.get("module", name) == module and attrs.get("enable", True) is not False:
            entry: dict[str, Any] = attrs
            return entry
    return None
