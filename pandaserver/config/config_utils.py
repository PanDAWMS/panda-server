import json
import os


# load configmap
def load_config_map(section, target_dict):
    if "PANDA_HOME" not in os.environ:
        return
    config_map_name = "panda_server_config.json"
    config_map_path = os.path.join(os.environ["PANDA_HOME"], "etc/config_json", config_map_name)
    if os.path.exists(config_map_path):
        with open(config_map_path) as f:
            tmp_data = json.load(f)
            if section in tmp_data:
                target_dict.update(tmp_data[section])
