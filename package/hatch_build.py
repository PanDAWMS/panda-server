import distutils
import getpass
import glob
import grp
import json
import os
import pwd
import re
import requests
import socket
import stat
import subprocess
import sys
import sysconfig


from hatchling.builders.hooks.plugin.interface import BuildHookInterface


def get_repo_info() -> object:
    # Get the current remote URL of the repository
    repo_url = subprocess.check_output(['git', 'config', '--get', 'remote.origin.url']).strip().decode()

    # Get the repo and branch name
    match = re.match(r'https://github.com/(.*).git@(.*)', repo_url)

    if match:
        repo_name = match.group(1)
        branch_name = match.group(2)
    else:
        repo_name = repo_url.rstrip('.git')
        branch_name = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode()

    # Commit hash
    commit_hash = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip().decode()

    return repo_name, branch_name, commit_hash

def mm_notification():

    repo_name, branch_name, commit_hash = get_repo_info()

    #Get Server Name
    server_name = socket.gethostname()

    # TODO: decide on the best path for the hook URL
    file_path = os.path.expanduser('~/mm_webhook_url.txt')
    with open(file_path, 'r') as file:
        mm_webhook_url = file.read().strip()
        if not mm_webhook_url:
            return

    mm_message = {
        "text": f"⚙️**Install Information.** **Package:** \"{repo_name[:7]}\u200B{repo_name[7:]}\". **Server Name:** {server_name}. **Branch:** {branch_name}. **Commit:** {commit_hash}."
    }
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(mm_webhook_url, data=json.dumps(mm_message), headers=headers)
        if response.status_code == 200:
            print("Message sent successfully to Mattermost")
        else:
            print(f"Failed to send message: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        # user
        if os.getgid() == 0:
            panda_user = "atlpan"
            panda_group = "zp"
        else:
            panda_user = getpass.getuser()
            panda_group = grp.getgrgid(os.getgid()).gr_name

        # parameters to be resolved
        self.params = {}
        self.params["install_dir"] = os.environ.get("PANDA_INSTALL_TARGET")
        if self.params["install_dir"]:
            # non-standard installation path
            self.params["install_purelib"] = self.params["install_dir"]
            self.params["install_scripts"] = os.path.join(self.params["install_dir"], "bin")
        else:
            self.params["install_dir"] = sys.prefix
            try:
                # python3.2 or higher
                self.params["install_purelib"] = sysconfig.get_path("purelib")
                self.params["install_scripts"] = sysconfig.get_path("scripts")
            except Exception:
                # old python
                self.params["install_purelib"] = distutils.sysconfig.get_python_lib()
                self.params["install_scripts"] = os.path.join(sys.prefix, "bin")
        for k in self.params:
            path = self.params[k]
            self.params[k] = os.path.abspath(os.path.expanduser(path))

        # other parameters
        self.params["panda_user"] = panda_user
        self.params["panda_group"] = panda_group
        self.params["python_exec_version"] = "%s.%s" % sys.version_info[:2]
        self.params["virtual_env"] = ""
        self.params["virtual_env_setup"] = ""
        if "VIRTUAL_ENV" in os.environ:
            self.params["virtual_env"] = os.environ["VIRTUAL_ENV"]
            self.params["virtual_env_setup"] = f"source {os.environ['VIRTUAL_ENV']}/bin/activate"
        elif sys.executable:
            venv_dir = os.path.dirname(os.path.dirname(sys.executable))
            py_venv_activate = os.path.join(venv_dir, "bin/activate")
            if os.path.exists(py_venv_activate):
                self.params["virtual_env"] = venv_dir
                self.params["virtual_env_setup"] = f"source {py_venv_activate}"

        # instantiate templates
        for in_f in glob.glob("./templates/**", recursive=True):
            if not in_f.endswith(".template"):
                continue
            with open(in_f) as in_fh:
                file_data = in_fh.read()
                # replace patterns
                for item in re.findall(r"@@([^@]+)@@", file_data):
                    if item not in self.params:
                        raise RuntimeError(f"unknown pattern {item} in {in_f}")
                    # get pattern
                    patt = self.params[item]
                    # convert to absolute path
                    if item.startswith("install"):
                        patt = os.path.abspath(patt)
                    # remove build/*/dump for bdist
                    patt = re.sub("build/[^/]+/dumb", "", patt)
                    # remove /var/tmp/*-buildroot for bdist_rpm
                    patt = re.sub("/var/tmp/.*-buildroot", "", patt)
                    # replace
                    file_data = file_data.replace(f"@@{item}@@", patt)
                out_f = re.sub(r"(\.exe)*\.template$", "", in_f)
                with open(out_f, "w") as out_fh:
                    out_fh.write(file_data)
                # chmod +x
                if in_f.endswith(".exe.template"):
                    tmp_st = os.stat(out_f)
                    os.chmod(out_f, tmp_st.st_mode | stat.S_IEXEC | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def finalize(self, version, build_data, artifact_path):
        # post install
        uid = pwd.getpwnam(self.params["panda_user"]).pw_uid
        gid = grp.getgrnam(self.params["panda_group"]).gr_gid
        for directory in ["/var/log/panda", "/var/log/panda/wsgisocks", "/var/log/panda/fastsocks"]:
            directory = self.params["virtual_env"] + directory
            if not os.path.exists(directory):
                os.makedirs(directory)
                os.chown(directory, uid, gid)
        if self.params["virtual_env"]:
            target_dir = os.path.join(self.params["virtual_env"], "etc/sysconfig")
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            target = os.path.join(target_dir, "panda_server")
            try:
                os.symlink(os.path.join(self.params["virtual_env"], "etc/panda/panda_server.sysconfig"), target)
            except Exception:
                pass

        # update the mattermost chat-ops channel
        mm_notification()
