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
if not tmpSelf.__dict__.has_key('dbtimeout'):
    tmpSelf.__dict__['dbtimeout'] = 60
    
# Directory for certs
if not tmpSelf.__dict__.has_key('certdir'):
    tmpSelf.__dict__['certdir'] = '/data/atlpan'

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
    
# default site
if not tmpSelf.__dict__.has_key('def_sitename'):
    tmpSelf.__dict__['def_sitename'] = 'BNL_ATLAS_1'
if not tmpSelf.__dict__.has_key('def_queue'):
    tmpSelf.__dict__['def_queue'] = 'ANALY_BNL_ATLAS_1'
if not tmpSelf.__dict__.has_key('def_nickname'):
    tmpSelf.__dict__['def_nickname']= 'BNL_ATLAS_1-condor'
if not tmpSelf.__dict__.has_key('def_dq2url'):
    tmpSelf.__dict__['def_dq2url']= 'http://dms02.usatlas.bnl.gov:8000/dq2/'
if not tmpSelf.__dict__.has_key('def_ddm'):
    tmpSelf.__dict__['def_ddm']= 'PANDA_UNDEFINED'
if not tmpSelf.__dict__.has_key('def_type'):
    tmpSelf.__dict__['def_type']= 'production'
if not tmpSelf.__dict__.has_key('def_gatekeeper'):
    tmpSelf.__dict__['def_gatekeeper'] = 'gridgk01.racf.bnl.gov'
if not tmpSelf.__dict__.has_key('def_status'):
    tmpSelf.__dict__['def_status'] = 'online'
    
if not tmpSelf.__dict__.has_key('memcached_sites'):
    tmpSelf.__dict__['memcached_sites'] = ['MWT2_UC',
                                      'ANALY_MWT2',
                                      'BNL_ATLAS_test',
                                      'ANALY_BNL_test',
                                      'ANALY_GLASGOW']

# dict for plugins
g_pluginMap = {}    

# parser for plugin setup
def parsePluginConf(modConfigName):
    global tmpSelf
    global g_pluginMap
    if not g_pluginMap.has_key(modConfigName):
        g_pluginMap[modConfigName] = {}
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
    except:
        pass


# accessor for plugin
def getPlugin(modConfigName,vo):
    if not g_pluginMap.has_key(modConfigName):
        return None
    elif g_pluginMap[modConfigName].has_key(vo):
        # VO specified
        return g_pluginMap[modConfigName][vo]
    elif g_pluginMap[modConfigName].has_key('any'):
        # catch all
        return g_pluginMap[modConfigName]['any']
    # undefined
    return None



# plug-ins
def setupPlugin():
    parsePluginConf('adder_plugins')
    parsePluginConf('setupper_plugins')
