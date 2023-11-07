import distutils
import getpass
import glob
import grp
import os
import pwd
import re
import stat
import sys
import sysconfig

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


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
