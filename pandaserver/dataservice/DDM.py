"""
provide primitive methods for DDM

"""

import re
import sys
import types
import commands
import hashlib
from config import panda_config

from rucio.client import Client as RucioClient
from rucio.common.exception import UnsupportedOperation,DataIdentifierNotFound,\
    FileAlreadyExists,Duplicate,DataIdentifierAlreadyExists,DuplicateRule


# change cwd
_cwd = 'cd %s > /dev/null 2>&1; export HOME=%s; ' % (panda_config.home_dir_cwd,panda_config.home_dir_cwd)

# environment variables
_env = 'PATH=%s:%s:$PATH '    % (panda_config.native_python,panda_config.globus_dir+'/bin')
_env+= 'LD_LIBRARY_PATH=%s '  % (panda_config.globus_dir+'/lib')
_env+= 'DQ2_HOME=%s/opt/dq2 ' % panda_config.dq2_dir
_env+= 'http_proxy=%s '       % panda_config.httpProxy
_env+= 'https_proxy=%s '      % panda_config.httpProxy

_env+= 'PYTHONPATH=%s/usr/lib/python2.3/site-packages:$PYTHONPATH' \
       % panda_config.dq2_dir

# method object wrapping DQ2 method
class _DQMethod:
    # constructor
    def __init__(self,moduleName,methodName):
        self.moduleName = moduleName
        self.methodName = methodName

    # method emulation
    def __call__(self,*args,**kwargs):
        # main method has disappeared since 0.3
        args = list(args)
        if self.methodName == 'main':
            self.methodName = args[0]
            args.pop(0)
        # build command
        com  = 'from dq2.clientapi.DQ2 import DQ2; '
        if 'force_backend' in kwargs and kwargs['force_backend'] != None:
            com += "dq2api = DQ2(force_backend='{0}'); ".format(kwargs['force_backend'])
        else:
            com += "dq2api = DQ2(force_backend='{0}'); ".format('rucio')
        try:
            del kwargs['force_backend']
        except:
            pass
        if self.moduleName == 'DQ2':
            # DQ2 is top-level module
            com += 'print dq2api.%s(' % self.methodName
        elif self.moduleName == 'DQ2_iter':
            # iterator
            com += 'iter = dq2api.%s(' % self.methodName
        else:
            com += 'print dq2api.%s.%s(' % (self.moduleName,self.methodName)
        # expand args
        for i in range(len(args)):
            arg = args[i]
            if isinstance(arg,types.UnicodeType):
                arg = str(arg)
            if isinstance(arg,types.StringType):
                # check invalid characters
                for invCh in ['"',"'",'(',')',';']:
                    if invCh in arg:
                        return -1,"invalid character %s in %s" % (invCh,arg)
                com = "%s'%s'," % (com,arg)
            else:
                com = '%s%s,' % (com,str(arg))
        for tmpK,tmpV in kwargs.iteritems():
            if isinstance(tmpV,types.UnicodeType):
                tmpV = str(tmpV)
            if isinstance(tmpV,types.StringType):
                com += "%s='%s'," % (tmpK,tmpV)
            else:
                com += "%s=%s," % (tmpK,tmpV)
        com = com[:-1]        
        com += ")"
        # loop over iterator
        if self.moduleName == 'DQ2_iter':
            com += ";exec 'for item in iter:print item'"
        # execute
        return commands.getstatusoutput('%s env %s python -c "%s"' % (_cwd,_env,com))
        

# DQ module class
class _DQModule:
    # constructor
    def __init__(self,moduleName):
        self.moduleName = moduleName

    # factory method
    def __getattr__(self,methodName):
        return _DQMethod(self.moduleName,methodName)


# native DQ2 method class
class NativeDQ2Method:
    # constructor
    def __init__(self):
        self.moduleName = None
        self.methodName = None
    # set module and method name
    def setNames(self,moduleName,methodName):
        self.moduleName = moduleName
        self.methodName = methodName
    # method emulation
    def __call__(self,*args,**kwargs):
        try:
            # make dq2api locally since global dq2 object is not thread-safe
            from dq2.clientapi import DQ2
            if 'force_backend' in kwargs and kwargs['force_backend'] != None:
                dq2api = DQ2.DQ2(force_backend=kwargs['force_backend'])
            else:
                dq2api = DQ2.DQ2(force_backend='rucio')
            try:
                del kwargs['force_backend']
            except:
                pass
            # main method has disappeared since 0.3
            args = list(args)
            if self.methodName == 'main':
                self.methodName = args[0]
                args.pop(0)
            # get method object
            if self.moduleName in ['DQ2','DQ2_iter']:
                methodObj = getattr(dq2api,self.methodName)
            else:
                methodObj = getattr(getattr(dq2api,self.moduleName),self.methodName)
            # execute
            retVal = apply(methodObj,args,kwargs)
            # loop over for iterator
            if self.moduleName == 'DQ2_iter':
                strRet = ''
                for item in retVal:
                    strRet += str(item)
            else:
                strRet = str(retVal)
            # return
            return 0,strRet
        except:
            errType,errVale = sys.exc_info()[:2]
            return 1,'%s %s' % (errType,errVale)
        
    
        
# native DQ2 module class
class NativeDQ2Module:
    # constructor
    def __init__(self):
        self.moduleName = None
    # set module name
    def setModName(self,moduleName):
        self.moduleName = moduleName
    # getter
    def __getattr__(self,methodName):
        # set method name
        api = NativeDQ2Method()        
        api.setNames(self.moduleName,methodName)
        return api
        

# factory class
class DDM:
    # constructor
    def __init__(self):
        self.usingNativeDQ2 = False
    # switch to use DQ2 in the same session
    def useDirectDQ2(self):
        self.usingNativeDQ2 = True
    # getter    
    def __getattr__(self,moduleName):
        if not self.usingNativeDQ2:
            # run dq2 comamnd in another session
            return _DQModule(moduleName)
        else:
            # run dq2 command in the same session
            nativeDQ2 = NativeDQ2Module()
            nativeDQ2.setModName(moduleName)
            return nativeDQ2

# instantiate
ddm = DDM()
del DDM


# method object wrapping TOA method
class _TOAMethod:
    # constructor
    def __init__(self,methodName):
        self.methodName = methodName

    # method emulation
    def __call__(self,*args):
        args = list(args)
        # build command
        com  = 'from dq2.info import TiersOfATLAS; '
        com += 'print TiersOfATLAS.%s(' % self.methodName
        # expand args
        for i in range(len(args)):
            arg = args[i]
            if isinstance(arg,types.StringType):
                com += "'%s'," % arg
            else:
                com += '%s,' % arg
        com = com[:-1]        
        com += ")"
        # execute
        return commands.getstatusoutput('%s env %s python -c "%s"' % (_cwd,_env,com))


# native ToA method class
class NativeTOAMethod:
    # constructor
    def __init__(self):
        self.methodName = None
        from dq2.info import TiersOfATLAS
        self.api = TiersOfATLAS
    # set method name
    def setName(self,methodName):
        self.methodName = methodName
    # method emulation
    def __call__(self,*args,**kwargs):
        try:
            methodObj = getattr(self.api,self.methodName)
            # execute
            retVal = apply(methodObj,args,kwargs)
            strRet = str(retVal)
            # return
            return 0,strRet
        except:
            errType,errVale = sys.exc_info()[:2]
            return 1,'%s %s' % (errType,errVale)


# TOA module class
class TOA:
    # constructor
    def __init__(self):
        self.usingNativeDQ2 = False
        self.nativeTOA = None
    # getter
    def __getattr__(self,methodName):
        if not ddm.usingNativeDQ2:
            # run dq2 comamnd in another session
            return _TOAMethod(methodName)
        else:
            # make method object
            if self.nativeTOA == None:
                self.nativeTOA = NativeTOAMethod()
            # run dq2 command in the same session
            self.nativeTOA.setName(methodName)
            return self.nativeTOA
                                

    
# instantiate
toa = TOA()
del TOA


# method object wrapping Dashboard method
class _DashBoradMethod:
    # constructor
    def __init__(self,methodName):
        self.methodName = methodName

    # method emulation
    def __call__(self,*args):
        args = list(args)
        # build command
        com  = "import sys;sys.stderr=open('/dev/null','w');"
        com += "import datetime;from dashboard.api.data.DataQuery import DataQuery;"
        com += "sys.stderr=sys.__stderr__;"
        com += "dash=DataQuery('dashb-atlas-data.cern.ch', 80);"
        com += "print dash.%s(%s,'%s'," % (self.methodName,args[0],args[1])
        com += "startDate=datetime.datetime.utcnow()-datetime.timedelta(hours=24))"
        # execute
        return commands.getstatusoutput('%s python -c "%s"' % (_cwd,com))


# TOA module class
class DashBorad:
    def __getattr__(self,methodName):
        return _DashBoradMethod(methodName)

# instantiate
dashBorad = DashBorad()
del DashBorad
    

# method object wrapping DQ2Info method
class _DQ2InfoMethod:
    # constructor
    def __init__(self,methodName):
        self.methodName = methodName

    # method emulation
    def __call__(self,*args):
        args = list(args)
        # build command
        com  = 'from dq2.info.client.infoClient import infoClient; '
        com += 'print infoClient().%s(' % self.methodName
        # expand args
        for i in range(len(args)):
            arg = args[i]
            if isinstance(arg,types.StringType):
                com += "'%s'," % arg
            else:
                com = '%s,' % arg
        com = com[:-1]        
        com += ")"
        # execute
        return commands.getstatusoutput('%s env %s python -c "%s"' % (_cwd,_env,com))


# TOA module class
class DQ2Info:
    def __getattr__(self,methodName):
        return _DQ2InfoMethod(methodName)

    
# instantiate
dq2Info = DQ2Info()
del DQ2Info


# method object wrapping dq2 common
class _DQ2CommonMethod:
    # constructor
    def __init__(self,methodName):
        self.methodName = methodName

    # method emulation
    def __call__(self,*args):
        args = list(args)
        # build command
        com  = 'from dq2.common import %s; ' % self.methodName
        com += 'print %s(' % self.methodName
        # expand args
        for i in range(len(args)):
            arg = args[i]
            if isinstance(arg,types.StringType):
                com += "'%s'," % arg
            else:
                com = '%s,' % arg
        com = com[:-1]        
        com += ")"
        # execute
        return commands.getstatusoutput('%s env %s python -c "%s"' % (_cwd,_env,com))


# TOA module class
class DQ2Common:
    def __getattr__(self,methodName):
        return _DQ2CommonMethod(methodName)

    
# instantiate
dq2Common = DQ2Common()
del DQ2Common



# rucio
class RucioAPI:
    # constructor
    def __init__(self):
        pass


    # extract scope
    def extract_scope(self,dsn):
        if ':' in dsn:
            return dsn.split(':')[:2]
        scope = dsn.split('.')[0]
        if dsn.startswith('user') or dsn.startswith('group'):
            scope = ".".join(dsn.split('.')[0:2])
        return scope,dsn


    # register dataset with existing files
    def registerDatasetWithOldFiles(self,dsn,lfns=[],guids=[],sizes=[],checksums=[],lifetime=None,scope=None):
        files = []
        for lfn, guid, size, checksum in zip(lfns, guids, sizes, checksums):
            if lfn.find(':') > -1:
                s, lfn = lfn.split(':')[0], lfn.split(':')[1]
            else:
                s = scope
            file = {'scope': s, 'name': lfn, 'bytes': size, 'meta': {'guid': guid}}
            if checksum.startswith('md5:'):
                file['md5'] = checksum[4:]
            elif checksum.startswith('ad:'):
                file['adler32'] = checksum[3:]
            files.append(file)
        # register dataset
        client = RucioClient()
        try:
            scope,dsn = self.extract_scope(dsn)
            client.add_dataset(scope=scope, name=dsn)
            if lifetime != None:
                client.set_metadata(scope,dsn,key='lifetime',value=lifetime*86400)
        except DataIdentifierAlreadyExists:
            pass
        # open dataset just in case
        try:
            client.set_status(scope,dsn,open=True)
        except:
            pass
        # add files
        try:
            client.add_files_to_dataset(scope=scope,name=dsn,files=files, rse=None)
        except FileAlreadyExists:
            for f in files:
                try:
                    client.add_files_to_dataset(scope=scope, name=dsn, files=[f], rse=None)
                except FileAlreadyExists:
                    pass
        vuid = hashlib.md5(scope+':'+dsn).hexdigest()
        vuid = '%s-%s-%s-%s-%s' % (vuid[0:8], vuid[8:12], vuid[12:16], vuid[16:20], vuid[20:32])
        duid = vuid
        return {'duid': duid, 'version': 1, 'vuid': vuid}



    # register dataset location
    def registerDatasetLocation(self,dsn,rses,lifetime=None,owner=None,activity=None):
        if lifetime != None:
            lifetime = lifetime*24*60*60
        scope,dsn = self.extract_scope(dsn)    
        dids = []
        did = {'scope': scope, 'name': dsn}
        dids.append(did)
        # make location
        rses.sort()
        location = '|'.join(rses)
        # check if a replication rule already exists
        client = RucioClient()
        # owner
        if owner == None:
            owner = client.account
        for rule in client.list_did_rules(scope=scope, name=dsn):
            if (rule['rse_expression'] == location) and (rule['account'] == client.account):
                return True
        try:
            client.add_replication_rule(dids=dids,copies=1,rse_expression=location,weight=None,
                                        lifetime=lifetime, grouping='DATASET', account=owner,
                                        locked=False,activity=activity,notify='N',
                                        ignore_availability=True)
        except (Duplicate,DuplicateRule):
            pass
        return True



    # register files in dataset
    def registerFilesInDataset(self,idMap):
        # loop over all rse
        attachmentList = []
        for rse,tmpMap in idMap.iteritems():
            # loop over all datasets
            for datasetName,fileList in tmpMap.iteritems():
                # extract scope from dataset
                scope,dsn = self.extract_scope(datasetName)
                files = []
                for tmpFile in fileList:
                    # extract scope from LFN if available
                    if 'name' in tmpFile:
                        lfn = tmpFile['name']
                    else:
                        lfn = tmpFile['lfn']
                    if ':' in lfn:
                        s, lfn = lfn.split(':')
                    else:
                        s = scope
                    # set metadata
                    meta = {}
                    if 'guid' in tmpFile:
                        meta['guid'] = tmpFile['guid']
                    if 'events' in tmpFile:
                        meta['events'] = tmpFile['events']
                    if 'lumiblocknr' in tmpFile:
                        meta['lumiblocknr'] = tmpFile['lumiblocknr']
                    if 'panda_id' in tmpFile:
                        meta['panda_id'] = tmpFile['panda_id']
                    if 'campaign' in tmpFile:
                        meta['campaign'] = tmpFile['campaign']
                    if 'bytes' in tmpFile:
                        fsize = tmpFile['bytes']
                    else:
                        fsize = tmpFile['size']
                    # set mandatory fields
                    file = {'scope': s,
                            'name' : lfn,
                            'bytes': fsize,
                            'meta' : meta}
                    if 'checksum' in tmpFile:
                        checksum = tmpFile['checksum']
                        if checksum.startswith('md5:'):
                            file['md5'] = checksum[4:]
                        elif checksum.startswith('ad:'):
                            file['adler32'] = checksum[3:]
                    if 'surl' in tmpFile:
                        file['pfn'] = tmpFile['surl']
                    # append files
                    files.append(file)
                # add attachment
                attachment = {'scope':scope,
                              'name':dsn,
                              'dids':files}
                if rse != None:
                    attachment['rse'] = rse
                attachmentList.append(attachment)
        # add files
        client = RucioClient()
        return client.add_files_to_datasets(attachmentList,ignore_duplicate=True)



    # get disk usage at RSE
    def getRseUsage(self,rse,src='srm'):
        retMap = {}
        try:
            client = RucioClient()
            itr = client.get_rse_usage(rse)
            # look for srm
            for item in itr:
                if item['source'] == src:
                    try:
                        total = item['total']/1024/1024/1024
                    except:
                        total = None
                    try:
                        used = item['used']/1024/1024/1024
                    except:
                        used = None
                    try:
                        free = item['free']/1024/1024/1024
                    except:
                        free = None
                    retMap = {'total':total,
                              'used':used,
                              'free':free}
                    break
        except:
            pass
        return retMap



    # list dataset replicas
    def listDatasetReplicas(self,datasetName):
        retMap = {}
        # extract scope from dataset
        scope,dsn = self.extract_scope(datasetName)
        try:
            # get replicas
            client = RucioClient()
            itr = client.list_dataset_replicas(scope,dsn)
            for item in itr:
                rse = item["rse"]
                retMap[rse] = [{'total':item["length"],
                                'found':item["available_length"],
                                'immutable':1}]
            return 0,retMap
        except:
            errType,errVale = sys.exc_info()[:2]
            return 1,'%s %s' % (errType,errVale)



    # set metadata
    def setMetaData(self,dsn,metadata=None):
        # register dataset
        client = RucioClient()
        try:
            scope,dsn = self.extract_scope(dsn)
            for tmpKey,tmpValue in metadata.iteritems():
                client.set_metadata(scope,dsn,key=tmpKey,value=tmpValue)
        except:
            pass
        return True



    # get metadata
    def getMetaData(self,dsn):
        # register dataset
        client = RucioClient()
        try:
            scope,dsn = self.extract_scope(dsn)
            return True,client.get_metadata(scope,dsn)
        except DataIdentifierNotFound:
            return True,None
        except:
            errType,errVale = sys.exc_info()[:2]
            return False,'%s %s' % (errType,errVale)



    # check if dataset exists
    def checkDatasetExist(self,dsn):
        # register dataset
        client = RucioClient()
        try:
            scope,dsn = self.extract_scope(dsn)
            return True
        except DataIdentifierNotFound:
            return False



    # close dataset
    def closeDataset(self,dsn):
        # register dataset
        client = RucioClient()
        try:
            scope,dsn = self.extract_scope(dsn)
            client.set_status(scope,dsn,open=False)
        except (UnsupportedOperation,DataIdentifierNotFound):
            pass
        return True



    # list file replicas
    def listFileReplicas(self,scopes,lfns):
        try:
            client = RucioClient()
            dids = []
            iGUID = 0
            nGUID = 1000
            retVal = {}
            for scope,lfn in zip(scopes,lfns):
                iGUID += 1
                dids.append({'scope':scope,'name':lfn})
                if len(dids) % nGUID == 0 or iGUID == len(lfns):
                    for tmpDict in client.list_replicas(dids,['srm']):
                        rses = tmpDict['rses'].keys()
                        if len(rses) > 0:
                            retVal[lfn] = rses
                    dids = []
            return True,retVal
        except:
            errType,errVale = sys.exc_info()[:2]
            return False,'%s %s' % (errType,errVale)


    
    # list files in dataset
    def listFilesInDataset(self,datasetName,long=False,fileList=None):
        # extract scope from dataset
        scope,dsn = self.extract_scope(datasetName)
        client = RucioClient()
        return_dict = {}
        for x in client.list_files(scope, dsn, long=long):
            tmpLFN = str(x['name'])
            if fileList != None:
                genLFN = re.sub('\.\d+$','',tmpLFN)
                if not tmpLFN in fileList and not genLFN in fileList:
                    continue
            dq2attrs = {}
            dq2attrs['chksum'] = "ad:" + str(x['adler32'])
            dq2attrs['md5sum'] = dq2attrs['chksum']
            dq2attrs['fsize'] = x['bytes']
            dq2attrs['scope'] = str(x['scope'])
            dq2attrs['events'] = str(x['events'])
            if long:
                dq2attrs['lumiblocknr'] = str(x['lumiblocknr'])
            guid = str('%s-%s-%s-%s-%s' % (x['guid'][0:8], x['guid'][8:12], x['guid'][12:16], x['guid'][16:20], x['guid'][20:32]))
            dq2attrs['guid'] = guid
            return_dict[tmpLFN] = dq2attrs
        return (return_dict, None)



    # register files
    def registerFiles(self,files,rse):
        client = RucioClient()
        try:
            # add replicas
            client.add_replicas(files=files,rse=rse)
        except FileAlreadyExists:
            pass
        try:
            # add rule
            client.add_replication_rule(files,copies=1,rse_expression=rse)
        except DuplicateRule:
            pass



    # delete files from dataset
    def deleteFilesFromDataset(self,datasetName,files):
        # extract scope from dataset
        scope,dsn = self.extract_scope(datasetName)
        client = RucioClient()
        try:
            # delete files
            client.detach_dids(scope=scope,name=dsn,dids=files)
        except DataIdentifierNotFound:
            pass



# instantiate
rucioAPI = RucioAPI()
del RucioAPI

