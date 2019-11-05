import re
import sys
import socket
from pandacommon.liveconfigparser.LiveConfigParser import LiveConfigParser

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read('panda_server.cfg')

# get server section
tmpDict = tmpConf.server

# expand all values
tmpSelf = sys.modules[ __name__ ]
for tmpKey in tmpDict:
    tmpVal = tmpDict[tmpKey]
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
if 'pserverhost' not in tmpSelf.__dict__:
    tmpSelf.__dict__['pserverhost'] = socket.getfqdn()

# set port for http
if 'pserverporthttp' not in tmpSelf.__dict__:
    tmpSelf.__dict__['pserverporthttp'] = 25080

# set host for http
if 'pserverhosthttp' not in tmpSelf.__dict__:
    tmpSelf.__dict__['pserverhosthttp'] = tmpSelf.__dict__['pserverhost']

# change the number of database connections for FastCGI/WSGI
if tmpSelf.__dict__['useFastCGI'] or tmpSelf.__dict__['useWSGI']:
    tmpSelf.__dict__['nDBConnection'] = tmpSelf.__dict__['nDBConForFastCGIWSGI']

# DB backend
if 'backend' not in tmpSelf.__dict__:
    tmpSelf.__dict__['backend'] = 'oracle'
if 'dbport' not in tmpSelf.__dict__:
    tmpSelf.__dict__['dbport'] = 0
if 'dbtimeout' not in tmpSelf.__dict__:
    tmpSelf.__dict__['dbtimeout'] = 60
    
# Directory for certs
if 'certdir' not in tmpSelf.__dict__:
    tmpSelf.__dict__['certdir'] = '/data/atlpan'

# endpoint map file
if 'endpoint_mapfile' not in tmpSelf.__dict__:
    tmpSelf.__dict__['endpoint_mapfile'] = '/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json'

# schemas
if 'schemaPANDA' not in tmpSelf.__dict__:
    tmpSelf.__dict__['schemaPANDA'] = 'ATLAS_PANDA'
if 'schemaPANDAARCH' not in tmpSelf.__dict__:
    tmpSelf.__dict__['schemaPANDAARCH'] = 'ATLAS_PANDAARCH'
if 'schemaMETA' not in tmpSelf.__dict__:
    tmpSelf.__dict__['schemaMETA'] = 'ATLAS_PANDAMETA'
if 'schemaJEDI' not in tmpSelf.__dict__:
    tmpSelf.__dict__['schemaJEDI'] = 'ATLAS_PANDA'
if 'schemaDEFT' not in tmpSelf.__dict__:
    tmpSelf.__dict__['schemaDEFT'] = 'ATLAS_DEFT'
if 'schemaGRISLI' not in tmpSelf.__dict__:
    tmpSelf.__dict__['schemaGRISLI'] = 'ATLAS_GRISLI'
if 'schemaEI' not in tmpSelf.__dict__:
    tmpSelf.__dict__['schemaEI'] = 'ATLAS_EVENTINDEX'
    
# default site
if 'def_sitename' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_sitename'] = 'BNL_ATLAS_1'
if 'def_queue' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_queue'] = 'ANALY_BNL_ATLAS_1'
if 'def_nickname' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_nickname']= 'BNL_ATLAS_1-condor'
if 'def_dq2url' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_dq2url']= 'http://dms02.usatlas.bnl.gov:8000/dq2/'
if 'def_ddm' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_ddm']= 'PANDA_UNDEFINED2'
if 'def_type' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_type']= 'production'
if 'def_gatekeeper' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_gatekeeper'] = 'gridgk01.racf.bnl.gov'
if 'def_status' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_status'] = 'online'
if 'token_authType' not in tmpSelf.__dict__:
    tmpSelf.__dict__['token_authType'] = None
if 'token_audience' not in tmpSelf.__dict__:
    tmpSelf.__dict__['token_audience'] = 'https://pandaserver.cern.ch'
if 'token_issuers' not in tmpSelf.__dict__:
    tmpSelf.__dict__['token_issuers'] = ''



# dict for plugins
g_pluginMap = {}    

# parser for plugin setup
def parsePluginConf(modConfigName):
    global tmpSelf
    global g_pluginMap
    g_pluginMap.setdefault(modConfigName, {})
    # parse plugin setup
    try:
        for configStr in getattr(tmpSelf,modConfigName).split(','):
            configStr = configStr.strip()
            items = configStr.split(':')
            vos          = items[0].split('|')
            moduleName   = items[1]
            className    = items[2]
            for vo in vos:
                # import
                mod = __import__(moduleName)
                for subModuleName in moduleName.split('.')[1:]:
                    mod = getattr(mod,subModuleName)
                # get class
                cls = getattr(mod,className)
                g_pluginMap[modConfigName][vo] = cls
    except Exception:
        pass


# accessor for plugin
def getPlugin(modConfigName,vo):
    if modConfigName not in g_pluginMap:
        return None
    elif vo in g_pluginMap[modConfigName]:
        # VO specified
        return g_pluginMap[modConfigName][vo]
    elif 'any' in g_pluginMap[modConfigName]:
        # catch all
        return g_pluginMap[modConfigName]['any']
    # undefined
    return None



# plug-ins
def setupPlugin():
    parsePluginConf('adder_plugins')
    parsePluginConf('setupper_plugins')
