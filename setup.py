#
#
# Setup prog for Panda Server
#
#
# set PYTHONPATH to use the current directory first
import sys
sys.path.insert(0,'.')

# get release version
import os
import PandaPkgInfo
release_version = PandaPkgInfo.release_version
if os.environ.has_key('BUILD_NUMBER'):
    release_version = '{0}.{1}'.format(release_version,os.environ['BUILD_NUMBER'])

import re
import sys
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
        self.prefix = '/data/atlpan/srv'


# generates files using templates and install them
class install_data_panda (install_data_org):

    def initialize_options (self):
        install_data_org.initialize_options (self)
        self.install_purelib = None
        self.host_name = socket.getfqdn()
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
                destFile = destFile.split('/')[-1]
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
                    # remove build/*/dump for bdist
                    patt = re.sub('build/[^/]+/dumb','',patt)
                    # remove /var/tmp/*-buildroot for bdist_rpm
                    patt = re.sub('/var/tmp/.*-buildroot','',patt)                    
                    # replace
                    filedata = filedata.replace('@@%s@@' % item, patt)
                # write to dest
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
               'pandaserver.userinterface',               
              ],
    data_files=[
                # config files 
                ('etc/panda', ['templates/panda_server-httpd.conf.rpmnew.template',
                               'templates/panda_server-httpd-FastCGI.conf.rpmnew.template',
                               'templates/panda_server.cfg.rpmnew.template',
                               'templates/panda_server-grid-env.sh.template',
                               ]
                 ),
                # sysconfig
                ('etc/sysconfig', ['templates/panda_server-sysconfig.rpmnew.template',
                                   ]
                 ),
                # logrotate
                ('etc/logrotate.d', ['templates/panda_server-logrotate.template',
                                     ]
                 ),
                # init script
                ('etc/init.d', ['templates/panda_server-ctl.exe.template',
                                   ]
                 ),
                # crons
                ('usr/bin', ['templates/panda_server-add.sh.exe.template',
                             'templates/panda_server-priority.sh.exe.template',
                             'templates/panda_server-copyArchive.sh.exe.template',
                             'templates/panda_server-copyROOT.sh.exe.template',
                             'templates/panda_server-vomsrenew.sh.exe.template',
                             'templates/panda_server-archivelog.sh.exe.template',
                             'templates/panda_server-tmpwatch.sh.exe.template',
                             'templates/panda_server-backupJobArch.sh.exe.template',
                             'templates/panda_server-deleteJobs.sh.exe.template',
                             'templates/panda_server-merge.sh.exe.template',
                             'templates/panda_server-datasetManager.sh.exe.template',
                             'templates/panda_server-evpPD2P.sh.exe.template',
                             'templates/panda_server-callback.sh.exe.template',
                             'templates/panda_server-makeSlsXml.exe.template',
                             'templates/panda_server-boostUser.sh.exe.template',
                             'templates/panda_server-runRebro.sh.exe.template',
                             ]
                 ),
                # var dirs
                #('var/log/panda', []),
                #('var/cache/pandaserver', []),                
                ],
    cmdclass={'install': install_panda,
              'install_data': install_data_panda}
)
