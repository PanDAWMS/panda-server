import re
import sys

from pandacommon.liveconfigparser.LiveConfigParser import (
    LiveConfigParser,
    expand_values,
)

from . import config_utils

# get ConfigParser
tmp_conf = LiveConfigParser()

# read
tmp_conf.read("panda_server.cfg")

# get daemon section
tmp_dict = getattr(tmp_conf, "mcp", {})

# read configmap
config_utils.load_config_map("mcp", tmp_dict)

# expand all values
tmp_self = sys.modules[__name__]
expand_values(tmp_self, tmp_dict)

# default values
if "transport" not in tmp_self.__dict__:
    tmp_self.__dict__["transport"] = "streamable-http"

if "endpoint_list_file" not in tmp_self.__dict__:
    tmp_self.__dict__["endpoint_list_file"] = "/opt/panda/etc/panda/panda_mcp_endpoints.json"

if "ssl_keyfile" not in tmp_self.__dict__:
    tmp_self.__dict__["ssl_keyfile"] = None

if "ssl_certfile" not in tmp_self.__dict__:
    tmp_self.__dict__["ssl_certfile"] = None
