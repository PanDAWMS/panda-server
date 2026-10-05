"""
download access tokens for OIDC token exchange flow
"""

import datetime
import fcntl
import json
import os.path
import pathlib
import re
import tempfile
from typing import Any

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore.oidc_utils import get_access_token
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

# logger
_logger = PandaLogger().getLogger("token_cache")


class TokenCache:
    """
    A class used to download and give access tokens for OIDC token exchange flow

    """

    # constructor
    def __init__(
        self, target_path: str | None = None, file_prefix: str | None = None, refresh_interval: int = 60, task_buffer: TaskBuffer | None = None
    ) -> None:
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
        if not os.path.exists(self.target_path):
            os.makedirs(self.target_path)
        if file_prefix:
            self.file_prefix = file_prefix
        else:
            self.file_prefix = "access_token_"
        self.refresh_interval = refresh_interval
        # minutes to wait before retrying a failed on-demand fetch
        self.failure_backoff = 5
        self.failed_fetches: dict[str, datetime.datetime] = {}
        self.task_buffer = task_buffer
        # cache for access tokens
        self.cached_access_tokens: dict[str, Any] = {}

    # construct target path
    def construct_target_path(self, client_name: str) -> str:
        """
        Constructs the target path to store an access token

        :param client_name: client name
        :return: the target path
        """
        return os.path.join(self.target_path, f"{self.file_prefix}{client_name}")

    # main
    def run(self) -> None:
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
                    # token file path
                    token_file_path = client_config.get("token_file_path")
                    if not token_file_path:
                        token_file_path = self.construct_target_path(client_name)
                    # check if fresh
                    is_fresh = False
                    if os.path.exists(token_file_path):
                        mod_time = datetime.datetime.fromtimestamp(os.stat(token_file_path).st_mtime, datetime.timezone.utc)
                        if datetime.datetime.now(datetime.timezone.utc) - mod_time < datetime.timedelta(minutes=self.refresh_interval):
                            tmp_log.debug(f"skip since {token_file_path} is fresh")
                            is_fresh = True
                    # tokens for entries with audience_from_request are fetched on demand by the API
                    audience_from_request = client_config.get("audience_from_request", False)
                    if audience_from_request:
                        tmp_log.debug(f"skip prefetch for {client_name} since audience comes from requests")
                    # get access token
                    if not is_fresh and not audience_from_request:
                        status_code, output = get_access_token(
                            client_config["endpoint"],
                            client_config["client_id"],
                            client_config["secret"],
                            scope=client_config.get("scope"),
                            audience=client_config.get("audience"),
                        )
                        if status_code:
                            with open(token_file_path, "w") as f:
                                f.write(output)
                            tmp_log.debug(f"dump access token to {token_file_path}")
                        else:
                            tmp_log.error(output)
                            # touch file to avoid immediate reattempt
                            pathlib.Path(token_file_path).touch()
                            tmp_log.debug(f"touch {token_file_path} to avoid immediate reattempt")
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
        time_now = naive_utcnow()
        if client_name in self.cached_access_tokens and self.cached_access_tokens[client_name]["last_update"] + datetime.timedelta(minutes=10) > time_now:
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
        cached_token: str | None = self.cached_access_tokens[client_name]["token"]
        return cached_token

    # construct the cache file path for a client and an audience
    def construct_audience_path(self, client_name: str, audience: str) -> str:
        """
        Construct the cache file path for a token of a client with a specific audience

        :param client_name: client name
        :param audience: audience of the token
        :return: the file path
        """
        safe_audience = re.sub(r"[^A-Za-z0-9._-]", "_", audience)
        return self.construct_target_path(f"{client_name}__{safe_audience}")

    # read a token file if it is younger than refresh_interval
    def _read_if_fresh(self, path: str) -> str | None:
        try:
            mod_time = datetime.datetime.fromtimestamp(os.stat(path).st_mtime, datetime.timezone.utc)
        except FileNotFoundError:
            return None
        if datetime.datetime.now(datetime.timezone.utc) - mod_time >= datetime.timedelta(minutes=self.refresh_interval):
            return None
        with open(path) as f:
            token = f.read()
        return token or None

    # get an access token for a client with an audience given in the request, fetching it on a cache miss
    def get_access_token_for_audience(self, client_name: str, client_config: dict[str, Any], audience: str) -> str | None:
        """
        Get an access token for a client with the audience given by the caller. The token is cached in a file
        shared by all processes. On a miss, one process fetches it under a file lock while others wait and reuse it.

        :param client_name: client name
        :param client_config: configuration of the client in token_cache_config
        :param audience: audience of the token, already validated by the caller
        :return: the access token or None if it could not be obtained
        """
        tmp_log = LogWrapper(_logger, f"get_access_token_for_audience client={client_name} aud={audience}")
        target_path = self.construct_audience_path(client_name, audience)
        token = self._read_if_fresh(target_path)
        if token:
            return token
        # back off after a recent failure to avoid hammering the token issuer
        last_failure = self.failed_fetches.get(target_path)
        if last_failure and naive_utcnow() - last_failure < datetime.timedelta(minutes=self.failure_backoff):
            tmp_log.debug("skip since the last attempt failed recently")
            return None
        with open(f"{target_path}.lock", "a") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                # another process may have fetched it while waiting for the lock
                token = self._read_if_fresh(target_path)
                if token:
                    return token
                status_code, output = get_access_token(
                    client_config["endpoint"],
                    client_config["client_id"],
                    client_config["secret"],
                    client_config.get("scope"),
                    audience=audience,
                )
                if not status_code:
                    tmp_log.error(output)
                    self.failed_fetches[target_path] = naive_utcnow()
                    return None
                fd, tmp_path = tempfile.mkstemp(dir=self.target_path, prefix=".tmp_")
                with os.fdopen(fd, "w") as f:
                    f.write(output)
                os.replace(tmp_path, target_path)
                self.failed_fetches.pop(target_path, None)
                tmp_log.debug(f"dump access token to {target_path}")
                return output
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
