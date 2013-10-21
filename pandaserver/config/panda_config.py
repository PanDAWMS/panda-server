import re
import sys
import commands
from liveconfigparser.LiveConfigParser import LiveConfigParser

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read('panda_server.cfg')

# get server section
tmpDict = tmpConf.server

# expand all values
tmpSelf = sys.modules[ __name__ ]
for tmpKey,tmpVal in tmpDict.iteritems():
    # convert string to bool/int
    if tmpVal == 'True':
        tmpVal = True
    elif tmpVal == 'False':
        tmpVal = False
    elif re.match('^\d+$',tmpVal):
        tmpVal = int(tmpVal)
    # update dict
    tmpSelf.__dict__[tmpKey] = tmpVal

# set hostname
if not tmpSelf.__dict__.has_key('pserverhost'):
    tmpSelf.__dict__['pserverhost'] = commands.getoutput('hostname -f')

# set port for http
if not tmpSelf.__dict__.has_key('pserverporthttp'):
    tmpSelf.__dict__['pserverporthttp'] = 25080

# set host for http
if not tmpSelf.__dict__.has_key('pserverhosthttp'):
    tmpSelf.__dict__['pserverhosthttp'] = tmpSelf.__dict__['pserverhost']

# change the number of database connections for FastCGI/WSGI
if tmpSelf.__dict__['useFastCGI'] or tmpSelf.__dict__['useWSGI']:
    tmpSelf.__dict__['nDBConnection'] = tmpSelf.__dict__['nDBConForFastCGIWSGI']

# DB backend
if not tmpSelf.__dict__.has_key('backend'):
    tmpSelf.__dict__['backend'] = 'oracle'
if not tmpSelf.__dict__.has_key('dbport'):
    tmpSelf.__dict__['dbport'] = 0


# schemas
if not tmpSelf.__dict__.has_key('schemaPANDA'):
    tmpSelf.__dict__['schemaPANDA'] = 'ATLAS_PANDA'
if not tmpSelf.__dict__.has_key('schemaPANDAARCH'):
    tmpSelf.__dict__['schemaPANDAARCH'] = 'ATLAS_PANDAARCH'
if not tmpSelf.__dict__.has_key('schemaMETA'):
    tmpSelf.__dict__['schemaMETA'] = 'ATLAS_PANDAMETA'
if not tmpSelf.__dict__.has_key('schemaJEDI'):
    tmpSelf.__dict__['schemaJEDI'] = 'ATLAS_PANDA'
if not tmpSelf.__dict__.has_key('schemaDEFT'):
    tmpSelf.__dict__['schemaDEFT'] = 'ATLAS_DEFT'
if not tmpSelf.__dict__.has_key('schemaGRISLI'):
    tmpSelf.__dict__['schemaGRISLI'] = 'ATLAS_GRISLI'
