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
    A class used to download and give access tokens for OIDC token exchange flow

    """

    # constructor
    def __init__(self, target_path: str = None, file_prefix: str = None, refresh_interval: int = 60, task_buffer=None):
        """
        Constructs all the necessary attributes for the TokenCache object.

        :param target_path: The base path to store the access tokens
        :param file_prefix: The prefix of the access token files
        :param refresh_interval: The interval to refresh the access tokens (default is 60 minutes)
        :param task_buffer: TaskBuffer object
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
        self.task_buffer = task_buffer
        # cache for access tokens
        self.cached_access_tokens = {}

    # construct target path
    def construct_target_path(self, client_name: str) -> str:
        """
        Constructs the target path to store an access token

        :param client_name: client name
        :return: the target path
        """
        return os.path.join(self.target_path, f"{self.file_prefix}{client_name}")

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
                    target_path = self.construct_target_path(client_name)
                    # check if fresh
                    is_fresh = False
                    if os.path.exists(target_path):
                        mod_time = datetime.datetime.fromtimestamp(os.stat(target_path).st_mtime, datetime.timezone.utc)
                        if datetime.datetime.now(datetime.timezone.utc) - mod_time < datetime.timedelta(minutes=self.refresh_interval):
                            tmp_log.debug(f"skip since {target_path} is fresh")
                            is_fresh = True
                    # get access token
                    if not is_fresh:
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
                            tmp_log.debug(f"touch {target_path} to avoid immediate reattempt")
                    # register token keys
                    if client_config.get("use_token_key") is True and self.task_buffer is not None:
                        token_key_lifetime = client_config.get("token_key_lifetime", 96)
                        tmp_log.debug(f"register token key for {client_name}")
                        tmp_stat = self.task_buffer.register_token_key(client_name, token_key_lifetime)
                        if not tmp_stat:
                            tmp_log.error("failed")
        except Exception as e:
            tmp_log.error(f"failed with {str(e)}")
        tmp_log.debug("================= end ==================")
        tmp_log.debug("done")
        return

    # get access token for a client
    def get_access_token(self, client_name: str) -> str | None:
        """
        Get an access token string for a client. None is returned if the access token is not found

        :param client_name : client name
        :return: the access token
        """
        time_now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        if client_name in self.cached_access_tokens and self.cached_access_tokens[client_name]["last_update"] + datetime.timedelta(minutes=10) < time_now:
            # use cached token since it is still fresh
            pass
        else:
            target_path = self.construct_target_path(client_name)
            token = None
            if os.path.exists(target_path):
                with open(target_path) as f:
                    token = f.read()
            if not token:
                token = None
            self.cached_access_tokens[client_name] = {"token": token, "last_update": time_now}
        return self.cached_access_tokens[client_name]["token"]
