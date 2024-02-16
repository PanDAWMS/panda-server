import datetime
import hashlib
import os
import shutil
import subprocess

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

_logger = PandaLogger().getLogger("ProxyCache")


def execute(program, log_stream):
    """Run a program on the command line. Return stderr, stdout and status."""
    log_stream.info(f"executable: {program}")
    pipe = subprocess.Popen(
        program,
        bufsize=-1,
        shell=True,
        close_fds=False,
        cwd="/tmp",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = pipe.communicate()
    return stdout, stderr, pipe.wait()


def cat(filename):
    """Given filename, print its text contents."""
    with open(filename, "r") as f:
        out = f.read()
    return out


class MyProxyInterface(object):
    """Class to store and retrieve proxies from my proxies."""

    def __init__(self):
        self.__target_path = "/tmp/proxies"
        self.__cred_name = "panda"
        if not os.path.exists(self.__target_path):
            os.makedirs(self.__target_path)

    def store(
        self,
        user_dn,
        cred_name,
        production=False,
        server_name="myproxy.cern.ch",
        role=None,
        log_stream=None,
    ):
        log_stream.info("store proxy")

        # Retrieve proxy from myproxy
        proxy_path = os.path.join(
            self.__target_path,
            hashlib.sha1(f"{user_dn}.plain".encode("utf-8")).hexdigest(),
        )

        # check if empty dummy file
        if os.path.exists(proxy_path) and os.stat(proxy_path).st_size == 0:
            if datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromtimestamp(
                os.path.getctime(proxy_path), datetime.timezone.utc
            ) < datetime.timedelta(hours=1):
                log_stream.info(f"skip too early to try again according to timestamp of {proxy_path}")
                return 2
        cmd = f"myproxy-logon -s {server_name} --no_passphrase --out {proxy_path} -l '{user_dn}' -k {cred_name} -t 0"

        stdout, stderr, status = execute(cmd, log_stream)
        if stdout:
            log_stream.info(f"stdout is {stdout} ")
        if stderr:
            log_stream.info(f"stderr is {stderr} ")
            # make a dummy to avoid too early reattempt
            open(proxy_path, "w").close()
        log_stream.info(f"test the status of plain... {status}")
        if status == 1:
            return status

        if role is not None:
            log_stream.info(f"proxy needs {role} - need to add voms attributes and store it in the cache")
            tmpExtension = self.getExtension(role)
            prodproxy_path = os.path.join(
                self.__target_path,
                str(hashlib.sha1((user_dn + tmpExtension).encode("utf-8")).hexdigest()),
            )
            prodcmd = f"voms-proxy-init -vomses /etc/vomses -valid 96:00 -rfc -cert {proxy_path} -key {proxy_path} -out {prodproxy_path} -n -voms {role}"
            stdout, stderr, status = execute(prodcmd, log_stream)
            if stdout:
                log_stream.info(f"stdout is {stdout} ")
            if stderr:
                log_stream.info(f"stderr is {stderr} ")
            log_stream.debug(f"test the status of production... {status}")
        elif production:
            log_stream.info("production proxy needed - need to add voms attributes and store it in the cache")
            prodproxy_path = os.path.join(
                self.__target_path,
                str(hashlib.sha1(f"{user_dn}.prod".encode("utf-8")).hexdigest()),
            )
            log_stream.info(prodproxy_path)
            prodcmd = f"voms-proxy-init -vomses /etc/vomses -valid 96:00 -rfc -cert {proxy_path} -key {proxy_path} -out {prodproxy_path} -n -voms atlas:/atlas/Role=production"
            stdout, stderr, status = execute(prodcmd, log_stream)
            if stdout:
                log_stream.info(f"stdout is {stdout} ")
            if stderr:
                log_stream.info(f"stderr is {stderr} ")
            log_stream.info(f"test the status of production... {status}")
        else:
            # Now we need to add atlas roles and store it
            atlasproxy_path = os.path.join(self.__target_path, hashlib.sha1(user_dn.encode("utf-8")).hexdigest())
            atlasrolescmd = f"voms-proxy-init -vomses /etc/vomses -valid 96:00 -rfc -cert {proxy_path} -key {proxy_path} -out {atlasproxy_path} -n -voms atlas"
            stdout, stderr, status = execute(atlasrolescmd, log_stream)
            if stdout:
                log_stream.info(f"stdout is {stdout} ")
            if stderr:
                log_stream.info(f"stderr is {stderr} ")
            log_stream.info(f"test the status of atlas... {status}")
        # make dummy to avoid too early attempts
        if status != 0 and not os.path.exists(proxy_path):
            open(proxy_path, "w").close()
        return status

    def retrieve(self, user_dn, production=False, role=None):
        """Retrieve proxy from proxy cache."""
        if role is not None:
            tmpExtension = self.getExtension(role)
            proxy_path = os.path.join(
                self.__target_path,
                str(hashlib.sha1((user_dn + tmpExtension).encode("utf-8")).hexdigest()),
            )
        elif production:
            proxy_path = os.path.join(
                self.__target_path,
                str(hashlib.sha1(f"{user_dn}.prod".encode("utf-8")).hexdigest()),
            )
        else:
            proxy_path = os.path.join(self.__target_path, hashlib.sha1(user_dn.encode("utf-8")).hexdigest())
        if os.path.isfile(proxy_path):
            return cat(proxy_path)
        else:
            _logger.warning(f"proxy file does not exist : DN:{user_dn} role:{role} file:{proxy_path}")

    # get proxy path
    def get_proxy_path(self, user_dn, production, role):
        if role is not None:
            tmpExtension = self.getExtension(role)
            return os.path.join(
                self.__target_path,
                str(hashlib.sha1((user_dn + tmpExtension).encode("utf-8")).hexdigest()),
            )
        elif production:
            return os.path.join(
                self.__target_path,
                str(hashlib.sha1(f"{user_dn}.prod".encode("utf-8")).hexdigest()),
            )
        else:
            return os.path.join(
                self.__target_path,
                hashlib.sha1(user_dn.encode("utf-8")).hexdigest(),
            )

    def checkProxy(self, user_dn, production=False, role=None, name=None):
        log_stream = LogWrapper(_logger, f'< name="{name}" role={role} >')
        log_stream.info(f"check proxy for {user_dn}")

        # Check the validity of a proxy
        proxy_path = self.get_proxy_path(user_dn, production, role)
        is_ok = False
        if os.path.isfile(proxy_path):
            log_stream.info("proxy is there. Need to check validity")
            cmd = f"voms-proxy-info -exists -hours 94 -file {proxy_path}"
            stdout, stderr, status = execute(cmd, log_stream)
            if stdout:
                log_stream.info(f"stdout is {stdout} ")
            if stderr:
                log_stream.info(f"stderr is {stderr} ")
            if status == 1:
                log_stream.info("proxy expires in 94h or less. We need to renew proxy!")
                ret = self.store(
                    user_dn,
                    self.__cred_name,
                    production,
                    role=role,
                    log_stream=log_stream,
                )
                if ret == 0:
                    log_stream.info("proxy retrieval successful")
                    # copy with compact name
                    alt_proxy_path = self.get_proxy_path(name, production, role)
                    shutil.copyfile(proxy_path, alt_proxy_path)
                    is_ok = True
                elif ret == 2:
                    log_stream.info("proxy retrieval on hold")
                else:
                    log_stream.error("proxy retrieval failed")
            else:
                log_stream.info("proxy is valid for more than 3 days")
                is_ok = True
        else:
            log_stream.info("proxy is not in the cache repo. will try to get it from myproxy")
            ret = self.store(user_dn, self.__cred_name, production, role=role, log_stream=log_stream)
            if ret == 0:
                log_stream.info("proxy stored successfully")
                alt_proxy_path = self.get_proxy_path(name, production, role)
                shutil.copyfile(proxy_path, alt_proxy_path)
                is_ok = True
            elif ret == 2:
                log_stream.info("proxy retrieval on hold")
            else:
                log_stream.error("proxy retrieval failed")
        if is_ok:
            plain_path = os.path.join(
                self.__target_path,
                hashlib.sha1(f"{user_dn}.plain".encode("utf-8")).hexdigest(),
            )
            if os.path.isfile(plain_path):
                return self.checkValidity(plain_path, log_stream)
            else:
                log_stream.error("plain proxy not there at the moment!")

    def checkValidity(self, proxy_path, log_stream):
        log_stream.info("Need to check validity and expiry!")
        time_left_thresholds = [24, 94, 168]
        status = 0
        for threshold in time_left_thresholds:
            cmd = f"voms-proxy-info -exists -hours {threshold} -file {proxy_path}"
            stdout, stderr, status = execute(cmd, log_stream)
            if status == 1:
                log_stream.warning(f"proxy expires in {threshold} hours")
                return threshold

        return status

    # get extension
    def getExtension(self, role):
        if role is not None:
            return "." + role.split("=")[-1]
        return None
