"""
download access tokens for OIDC token exchange flow
"""
import datetime
import json
import os.path
import pathlib

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.srvcore.oidc_utils import get_access_token

# logger
_logger = PandaLogger().getLogger("token_cache")


class TokenCache:
    """
    A class used to download access tokens for OIDC token exchange flow

    """

    # constructor
    def __init__(self, target_path=None, file_prefix=None, refresh_interval=60):
        """
        Constructs all the necessary attributes for the TokenCache object.
        Attributes:
            target_path : str
                The path to store the access tokens
            refresh_interval : int
                The interval to refresh the access tokens (default is 60 minutes)
        """
        if target_path:
            self.target_path = target_path
        else:
            self.target_path = "/tmp/proxies"
        if file_prefix:
            self.file_prefix = file_prefix
        else:
            self.file_prefix = "access_token_"
        self.refresh_interval = refresh_interval

    # main
    def run(self):
        """ "
        Main function to download access tokens
        """
        tmp_log = LogWrapper(_logger)
        tmp_log.debug("================= start ==================")
        try:
            # check config
            if not hasattr(panda_config, "token_cache_config") or not panda_config.token_cache_config:
                tmp_log.debug("token_cache_config is not set in panda_config")
            # check config path
            elif not os.path.exists(panda_config.token_cache_config):
                tmp_log.debug(f"config file {panda_config.token_cache_config} not found")
            # read config
            else:
                with open(panda_config.token_cache_config) as f:
                    token_cache_config = json.load(f)
                for client_name, client_config in token_cache_config.items():
                    tmp_log.debug(f"client_name={client_name}")
                    # target path
                    target_path = os.path.join(self.target_path, f"{self.file_prefix}{client_name}")
                    # check if fresh
                    if os.path.exists(target_path):
                        mod_time = datetime.datetime.fromtimestamp(os.stat(target_path).st_mtime, datetime.timezone.utc)
                        if datetime.datetime.now(datetime.timezone.utc) - mod_time < datetime.timedelta(minutes=self.refresh_interval):
                            tmp_log.debug(f"skip since {target_path} is fresh")
                            continue
                    # get access token
                    status_code, output = get_access_token(
                        client_config["endpoint"], client_config["client_id"], client_config["secret"], client_config.get("scope")
                    )
                    if status_code:
                        with open(target_path, "w") as f:
                            f.write(output)
                        tmp_log.debug(f"dump access token to {target_path}")
                    else:
                        tmp_log.error(output)
                        # touch file to avoid immediate reattempt
                        pathlib.Path(target_path).touch()
        except Exception as e:
            tmp_log.error(f"failed with {str(e)}")
        tmp_log.debug("================= end ==================")
        tmp_log.debug("done")
        return
