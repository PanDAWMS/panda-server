#
#
# Setup prog for Panda Server
#
#

# set PYTHONPATH to use the current directory first
import sys
sys.path.insert(0,'.')  # noqa: E402

import os
import pwd
import grp
import re
import site
import stat
import getpass
import socket
import distutils
from setuptools import setup
from setuptools.command.install import install as install_org
from distutils.command.install_data import install_data as install_data_org

# package version
import PandaPkgInfo
release_version = PandaPkgInfo.release_version

# user
if os.getgid() == 0:
    panda_user = 'atlpan'
    panda_group = 'zp'
else:
    panda_user = getpass.getuser()
    panda_group = grp.getgrgid(os.getgid()).gr_name

# get panda specific params
optPanda = {}
newArgv  = []
idx = 0
while idx < len(sys.argv):
    tmpArg = sys.argv[idx]
    if tmpArg.startswith('--panda_'):
        # panda params
        idx += 1
        if len(tmpArg.split('=')) == 2:
            # split to par and val if = is contained
            tmpVal = tmpArg.split('=')[-1]
            tmpArg = tmpArg.split('=')[0]
        elif len(tmpArg.split('=')) == 1:
            tmpVal = sys.argv[idx]
            idx += 1
        else:
            raise RuntimeError("invalid panda option : %s" % tmpArg)
        # get key
        tmpKey = re.sub('--panda_','',tmpArg)
        # set params
        optPanda[tmpKey] = tmpVal
    else:
        # normal opts
        idx += 1
        newArgv.append(tmpArg)
# set new argv
sys.argv = newArgv


# set overall prefix for bdist_rpm
class install_panda(install_org):
    def initialize_options (self):
        install_org.initialize_options(self)

    # disable egg
    def finalize_options(self):
        install_org.finalize_options(self)
        self.single_version_externally_managed = True

# generates files using templates and install them
class install_data_panda (install_data_org):

    def initialize_options (self):
        install_data_org.initialize_options (self)
        self.install_purelib = None
        self.panda_user = panda_user
        self.panda_group = panda_group
        self.python_exec_version = '%s.%s' % sys.version_info[:2]
        self.virtual_env = ''
        self.virtual_env_setup = ''
        if 'VIRTUAL_ENV' in os.environ:
            self.virtual_env = os.environ['VIRTUAL_ENV']
            self.virtual_env_setup = 'source {0}/bin/activate'.format(os.environ['VIRTUAL_ENV'])
        elif sys.executable:
            venv_dir = os.path.dirname(os.path.dirname(sys.executable))
            py_venv_activate = os.path.join(venv_dir, 'bin/activate')
            if os.path.exists(py_venv_activate):
                self.virtual_env = venv_dir
                self.virtual_env_setup = 'source {0}'.format(py_venv_activate)

    def finalize_options (self):
        # set install_purelib
        self.set_undefined_options('install',
                                   ('install_purelib', 'install_purelib'))
        # set prefix for pip
        if not hasattr(self, 'prefix'):
            self.prefix = site.PREFIXES[0]
        # set reaming params
        install_data_org.finalize_options(self)
        # set hostname
        if 'hostname' in optPanda and optPanda['hostname'] != '':
            self.hostname = optPanda['hostname']
        else:
            self.hostname = socket.getfqdn()
        # set user and group
        if 'username' in optPanda and optPanda['username'] != '':
            self.username  = optPanda['username']
        else:
            self.username  = getpass.getuser()
        if 'usergroup' in optPanda and optPanda['usergroup'] != '':
            self.usergroup = optPanda['usergroup']
        else:
            self.usergroup = grp.getgrgid(os.getgid()).gr_name


    def run (self):
        # setup.py install sets install_dir to /usr
        if self.install_dir == '/usr':
            self.install_dir = '/'
        elif 'bdist_wheel' in self.distribution.get_cmdline_options():
            # wheel
            self.install_dir = self.prefix
            self.install_purelib = distutils.sysconfig.get_python_lib()
            self.install_scripts = os.path.join(self.prefix, 'bin')
        if not self.install_dir:
            if self.root:
                # rpm
                self.install_dir = self.prefix
                self.install_purelib = distutils.sysconfig.get_python_lib()
                self.install_scripts = os.path.join(self.prefix, 'bin')
            else:
                # sdist
                if not self.prefix:
                    if '--user' in self.distribution.script_args:
                        self.install_dir = site.USER_BASE
                    else:
                        self.install_dir = site.PREFIXES[0]
                else:
                    self.install_dir = self.prefix
        # remove /usr for bdist/bdist_rpm
        match = re.search('(build/[^/]+/dumb)/usr',self.install_dir)
        if match is not None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # remove /var/tmp/*-buildroot for bdist_rpm
        match = re.search('(/var/tmp/.*-buildroot)/usr',self.install_dir)
        if match is not None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # create tmp area
        tmpDir = 'build/tmp'
        self.mkpath(tmpDir)
        new_data_files = []
        for destDir,dataFiles in self.data_files:
            newFilesList = []
            for srcFile in dataFiles:
                # check extension
                if not srcFile.endswith('.template'):
                    raise RuntimeError("%s doesn't have the .template extension" % srcFile)
                # dest filename
                destFile = re.sub('(\.exe)*\.template$','',srcFile)
                destFile = re.sub(r'^templates/','',destFile)
                destFile = '%s/%s' % (tmpDir,destFile)
                # open src
                inFile = open(srcFile)
                # read
                filedata=inFile.read()
                # close
                inFile.close()
                # replace patterns
                for item in re.findall('@@([^@]+)@@',filedata):
                    if not hasattr(self,item):
                        raise RuntimeError('unknown pattern %s in %s' % (item,srcFile))
                    # get pattern
                    patt = getattr(self,item)
                    # remove install root, if any
                    if self.root is not None and patt.startswith(self.root):
                        patt = patt[len(self.root):]
                    # remove build/*/dump for bdist
                    patt = re.sub('build/[^/]+/dumb','',patt)
                    # remove /var/tmp/*-buildroot for bdist_rpm
                    patt = re.sub('/var/tmp/.*-buildroot','',patt)
                    # replace
                    filedata = filedata.replace('@@%s@@' % item, patt)
                # write to dest
                if '/' in destFile:
                    destSubDir = os.path.dirname(destFile)
                    if not os.path.exists(destSubDir):
                        os.makedirs(destSubDir)
                oFile = open(destFile,'w')
                oFile.write(filedata)
                oFile.close()
                # chmod for exe
                if srcFile.endswith('.exe.template'):
                    tmp_st = os.stat(destFile)
                    os.chmod(destFile, tmp_st.st_mode | stat.S_IEXEC | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                # append
                newFilesList.append(destFile)
            # replace dataFiles to install generated file
            new_data_files.append((destDir,newFilesList))
        # install
        self.data_files = new_data_files
        install_data_org.run(self)

        # post install
        uid = pwd.getpwnam(panda_user).pw_uid
        gid = grp.getgrnam(panda_group).gr_gid
        for directory in ['/var/log/panda', '/var/log/panda/wsgisocks', '/var/log/panda/fastsocks']:
            directory = self.virtual_env + directory
            if not os.path.exists(directory):
                os.makedirs(directory)
                os.chown(directory, uid, gid)
        if self.virtual_env != '':
            target_dir = os.path.join(self.virtual_env, 'etc/sysconfig')
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            target = os.path.join(target_dir,'panda_server')
            try:
                os.symlink(os.path.join(self.virtual_env, 'etc/panda/panda_server.sysconfig'),
                           target)
            except Exception:
                pass



# setup for distutils
setup(
    name="panda-server",
    version=release_version,
    description=' PanDA Server Package',
    long_description='''This package contains PanDA Server Components''',
    license='GPL',
    author='Panda Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://twiki.cern.ch/twiki/bin/view/Atlas/PanDA',
    zip_safe=False,
    install_requires=['panda-common>=0.0.31',
                      'panda-client',
                      'pyOpenSSL',
                      'python-daemon',
                      'mod_wsgi',
                      'six',
                      'sqlalchemy',
                      'stomp.py',
                      'pyyaml',
                      'pyjwt',
                      'requests',
                      'psutil>=5.4.8',
                      'idds-common',
                      'idds-client',
                      'idds-workflow>=1.0.6',
                      'idds-doma',
                      'idds-atlas',
                      'ruamel.yaml',
                      'cwl-utils>=0.13',
                      'packaging'
                      ],
    extras_require={
        'oracle': ['cx_Oracle'],
        'mysql': ['mysqlclient'],
        'postgres': ['psycopg2-binary'],
        'rucio': ['rucio-clients'],
        'elasticsearch': ['elasticsearch'],
        'atlasprod': ['cx_Oracle', 'rucio-clients', 'elasticsearch', 'numpy', 'scipy'],
    },
    packages=[ 'pandaserver',
               'pandaserver.brokerage',
               'pandaserver.config',
               'pandaserver.daemons',
               'pandaserver.daemons.scripts',
               'pandaserver.dataservice',
               'pandaserver.jobdispatcher',
               'pandaserver.server',
               'pandaserver.srvcore',
               'pandaserver.taskbuffer',
               'pandaserver.test',
               'pandaserver.test.lsst',
               'pandaserver.test.alice',
               'pandaserver.userinterface',
               'pandaserver.proxycache',
               'pandaserver.configurator',
               'pandaserver.workflow'
              ],
    package_data = {'pandaserver.server': ['.gacl']},
    data_files=[
                # config files
                ('etc/panda', ['templates/panda_server-httpd.conf.rpmnew.template',
                               'templates/panda_server-httpd-FastCGI.conf.rpmnew.template',
                               'templates/panda_server.cfg.rpmnew.template',
                               'templates/pandasrv.cron.template',
                               'templates/logrotate.d/panda_server.logrotate.template',
                               'templates/sysconfig/panda_server.sysconfig.rpmnew.template'
                               ]
                 ),

                # init script
                ('etc/rc.d/init.d', ['templates/init.d/panda_server.exe.template',
                                     'templates/init.d/panda_httpd.exe.template',
                                     'templates/init.d/panda_daemon.exe.template',
                                   ]
                 ),
                # crons
                ('usr/bin', ['templates/panda_server-add_main.exe.template',
                             'templates/panda_server-add_sub.exe.template',
                             'templates/panda_server-priority.exe.template',
                             'templates/panda_server-copyArchive.exe.template',
                             'templates/panda_server-vomsrenew.exe.template',
                             'templates/panda_server-tmpwatch.exe.template',
                             'templates/panda_server-datasetManager.exe.template',
                             'templates/panda_server-evpPD2P.exe.template',
                             'templates/panda_server-callback.exe.template',
                             'templates/panda_server-makeSlsXml.exe.template',
                             'templates/panda_server-boostUser.exe.template',
                             'templates/panda_server-proxyCache.exe.template',
                             'templates/panda_server-configurator.exe.template',
                             'templates/panda_server-network_configurator.exe.template',
                             'templates/panda_server-schedconfig_json.exe.template',
                             'templates/panda_server-sw_tags.exe.template',
                             'templates/panda_server-esPreemption.exe.template',
                             'templates/panda_server-pilot_streaming.exe.template',
                             'templates/panda_server-frontier_retagging.exe.template'
                             ]
                 ),

                # var dirs
                #('var/log/panda', []),
                #('var/cache/pandaserver', []),
                ],
    cmdclass={'install': install_panda,
              'install_data': install_data_panda}
)
