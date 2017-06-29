#
#
# Setup prog for Panda Server
#
#

release_version='0.0.2'
panda_user = 'atlpan'
panda_group = 'zp'

# set PYTHONPATH to use the current directory first
import sys
sys.path.insert(0,'.')

import os
import pwd
import grp
import re
import socket
import commands
from distutils.core import setup
from distutils.command.install import install as install_org
from distutils.command.install_data import install_data as install_data_org

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
            raise RuntimeError,"invalid panda option : %s" % tmpArg
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

# generates files using templates and install them
class install_data_panda (install_data_org):

    def initialize_options (self):
        install_data_org.initialize_options (self)
        self.install_purelib = None
        self.panda_user = panda_user
        self.panda_group = panda_group
        self.python_exec_version = '%s.%s' % sys.version_info[:2]
        
    def finalize_options (self):
        # set install_purelib
        self.set_undefined_options('install',
                                   ('install_purelib','install_purelib'))
        # set reaming params
        install_data_org.finalize_options(self)
        # set hostname
        if optPanda.has_key('hostname') and optPanda['hostname'] != '':
            self.hostname = optPanda['hostname']
        else:
            self.hostname = commands.getoutput('hostname -f')
        # set user and group
        if optPanda.has_key('username') and optPanda['username'] != '':
            self.username  = optPanda['username']
        else:
            self.username  = commands.getoutput('id -un')
        if optPanda.has_key('usergroup') and optPanda['usergroup'] != '':
            self.usergroup = optPanda['usergroup']
        else:
            self.usergroup = commands.getoutput('id -gn')             
        
    
    def run (self):
        # remove /usr for bdist/bdist_rpm
        match = re.search('(build/[^/]+/dumb)/usr',self.install_dir)
        if match != None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # remove /var/tmp/*-buildroot for bdist_rpm
        match = re.search('(/var/tmp/.*-buildroot)/usr',self.install_dir)
        if match != None:
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
                    raise RuntimeError,"%s doesn't have the .template extension" % srcFile
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
                        raise RuntimeError,'unknown pattern %s in %s' % (item,srcFile)
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
                    commands.getoutput('chmod +x %s' % destFile)
                # append
                newFilesList.append(destFile)
            # replace dataFiles to install generated file
            new_data_files.append((destDir,newFilesList))
        # install
        self.data_files = new_data_files
        install_data_org.run(self)
        
        #post install
        uid = pwd.getpwnam(panda_user).pw_uid
        gid = grp.getgrnam(panda_group).gr_gid
        for directory in ['/var/log/panda', '/var/log/panda/wsgisocks', '/var/log/panda/fastsocks']:
            if not os.path.exists(directory):
                os.makedirs(directory)
                os.chown(directory, uid, gid)                
        
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
    packages=[ 'pandaserver',
               'pandaserver.brokerage',
               'pandaserver.config',
               'pandaserver.dataservice',
               'pandaserver.jobdispatcher',
               'pandaserver.server',
               'pandaserver.taskbuffer',
               'pandaserver.test',
               'pandaserver.test.lsst',
               'pandaserver.test.alice',
               'pandaserver.userinterface',
               'pandaserver.proxycache',
               'pandaserver.configurator'
              ],
    package_data = {'pandaserver.server': ['.gacl']},
    data_files=[
                # config files 
                ('/etc/panda', ['templates/panda_server-httpd.conf.rpmnew.template',
                               'templates/panda_server-httpd-FastCGI.conf.rpmnew.template',
                               'templates/panda_server.cfg.rpmnew.template',
                               'templates/panda_server-grid-env.template',
                               'templates/pandasrv.cron.template',
                               'templates/logrotate.d/panda_server.logrotate.template',
                               'templates/sysconfig/panda_server.sysconfig.template'
                               ]
                 ),

                # init script
                ('/etc/rc.d/init.d', ['templates/init.d/panda_server.exe.template',
                                   ]
                 ),
                # crons
                ('/usr/bin', ['templates/panda_server-add.exe.template',
                             'templates/panda_server-priority.exe.template',
                             'templates/panda_server-copyArchive.exe.template',
                             'templates/panda_server-copyROOT.exe.template',
                             'templates/panda_server-vomsrenew.exe.template',
                             'templates/panda_server-archivelog.exe.template',
                             'templates/panda_server-tmpwatch.exe.template',
                             'templates/panda_server-backupJobArch.exe.template',
                             'templates/panda_server-deleteJobs.exe.template',
                             'templates/panda_server-merge.exe.template',
                             'templates/panda_server-datasetManager.exe.template',
                             'templates/panda_server-evpPD2P.exe.template',
                             'templates/panda_server-callback.exe.template',
                             'templates/panda_server-makeSlsXml.exe.template',
                             'templates/panda_server-boostUser.exe.template',
                             'templates/panda_server-runRebro.exe.template',
                             'templates/panda_server-proxyCache.exe.template',
                             'templates/panda_server-shareMgr.exe.template',
                             'templates/panda_server-configurator.exe.template',
                             'templates/panda_server-esPreemption.exe.template',
                             'templates/panda_server-harvesterCtl.exe.template',
                             ]
                 ),

                # var dirs
                #('var/log/panda', []),
                #('var/cache/pandaserver', []),                
                ],
    cmdclass={'install': install_panda,
              'install_data': install_data_panda}
)
