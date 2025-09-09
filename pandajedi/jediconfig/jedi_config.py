import json
import os
import sys

from liveconfigparser.LiveConfigParser import LiveConfigParser, expand_values

# get ConfigParser
tmp_conf = LiveConfigParser()

# read
tmp_conf.read("panda_jedi.cfg")


# dummy section class
class _SectionClass:
    pass


# load configmap
config_map_data = {}
if "PANDA_HOME" in os.environ:
    config_map_name = "panda_jedi_config.json"
    config_map_path = os.path.join(os.environ["PANDA_HOME"], "etc/config_json", config_map_name)
    if os.path.exists(config_map_path):
        with open(config_map_path) as f:
            config_map_data = json.load(f)

# loop over all sections
for tmp_section in tmp_conf.sections():
    # read section
    tmp_dict = getattr(tmp_conf, tmp_section)
    # load configmap
    if tmp_section in config_map_data:
        tmp_dict.update(config_map_data[tmp_section])
    # make section class
    tmp_self = _SectionClass()
    # update module dict
    sys.modules[__name__].__dict__[tmp_section] = tmp_self
    # expand all values
    expand_values(tmp_self, tmp_dict)
