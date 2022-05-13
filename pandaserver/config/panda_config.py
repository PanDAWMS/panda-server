import os
import re
import sys
import json
import socket
import glob
from pandacommon.liveconfigparser.LiveConfigParser import LiveConfigParser
from . import config_utils

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read('panda_server.cfg')

# get server section
tmpDict = tmpConf.server

# read configmap
config_utils.load_config_map('server', tmpDict)

# expand all values
tmpSelf = sys.modules[ __name__ ]
for tmpKey in tmpDict:
    tmpVal = tmpDict[tmpKey]
    # convert string to bool/int
    if tmpVal == 'True':
        tmpVal = True
    elif tmpVal == 'False':
        tmpVal = False
    elif tmpVal == 'None':
        tmpVal = None
    elif isinstance(tmpVal, str) and re.match('^\d+$',tmpVal):
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
    tmpSelf.__dict__['endpoint_mapfile'] = '/cvmfs/atlas.cern.ch/repo/sw/local/etc/cric_ddmendpoints.json'\
                                           ',/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json'

# sandbox info
if 'record_sandbox_info' not in tmpSelf.__dict__:
    tmpSelf.__dict__['record_sandbox_info'] = True

# secrets
if 'pilot_secrets' not in tmpSelf.__dict__:
    tmpSelf.__dict__['pilot_secrets'] = 'pilot secrets'

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
    tmpSelf.__dict__['def_nickname'] = 'BNL_ATLAS_1-condor'
if 'def_dq2url' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_dq2url'] = 'http://dms02.usatlas.bnl.gov:8000/dq2/'
if 'def_ddm' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_ddm'] = 'PANDA_UNDEFINED2'
if 'def_type' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_type'] = 'production'
if 'def_status' not in tmpSelf.__dict__:
    tmpSelf.__dict__['def_status'] = 'online'
if 'token_authType' not in tmpSelf.__dict__:
    tmpSelf.__dict__['token_authType'] = 'oidc'
if 'auth_config' not in tmpSelf.__dict__:
    tmpSelf.__dict__['auth_config'] = '/opt/panda/etc/panda/auth/'
if 'auth_policies' not in tmpSelf.__dict__:
    tmpSelf.__dict__['auth_policies'] = '/opt/panda/etc/panda/auth_policies.json'
if 'token_audience' not in tmpSelf.__dict__:
    tmpSelf.__dict__['token_audience'] = 'https://pandaserver.cern.ch'
if 'token_issuers' not in tmpSelf.__dict__:
    tmpSelf.__dict__['token_issuers'] = ''
tmpSelf.__dict__['production_dns'] = [x for x in tmpSelf.__dict__.get('production_dns', '').split(',') if x]
try:
    data_dict = {}
    for name in glob.glob(os.path.join(tmpSelf.__dict__['auth_config'], '*_auth_config.json')):
        with open(name) as f:
            data = json.load(f)
            data_dict[data['client_id']] = data
    tmpSelf.__dict__['auth_config'] = data_dict
except Exception:
    tmpSelf.__dict__['auth_config'] = {}
try:
    with open(tmpSelf.__dict__['auth_policies']) as f:
        tmpSelf.__dict__['auth_policies'] = json.load(f)
except Exception:
    tmpSelf.__dict__['auth_policies'] = {}

# database info via env
if 'PANDA_DBHOST' in os.environ:
    tmpSelf.dbhost = os.environ['PANDA_DBHOST']
if 'PANDA_PASSWORD' in os.environ:
    tmpSelf.dbpasswd = os.environ['PANDA__PASSWORD']

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
            if len(items) > 3:
                group = items[3]
            else:
                group = None
            for vo in vos:
                # import
                mod = __import__(moduleName)
                for subModuleName in moduleName.split('.')[1:]:
                    mod = getattr(mod,subModuleName)
                # get class
                cls = getattr(mod,className)
                if group:
                    vo_key = '{}_{}'.format(vo, group)
                else:
                    vo_key = vo
                g_pluginMap[modConfigName][vo_key] = cls
    except Exception:
        pass


# accessor for plugin
def getPlugin(modConfigName, vo, group=None):
    if modConfigName not in g_pluginMap:
        return None
    if group:
        vo_group = '{}_{}'.format(vo, group)
        if vo_group in g_pluginMap[modConfigName]:
            # VO+group specified
            return g_pluginMap[modConfigName][vo_group]
    if vo in g_pluginMap[modConfigName]:
        # VO specified
        return g_pluginMap[modConfigName][vo]
    if 'any' in g_pluginMap[modConfigName]:
        # catch all
        return g_pluginMap[modConfigName]['any']
    # undefined
    return None



# plug-ins
def setupPlugin():
    parsePluginConf('adder_plugins')
    parsePluginConf('setupper_plugins')
