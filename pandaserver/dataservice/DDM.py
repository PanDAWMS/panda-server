"""
provide primitive methods for DDM

"""

import re
import sys
import hashlib

from rucio.client import Client as RucioClient
from rucio.common.exception import UnsupportedOperation,DataIdentifierNotFound,\
    FileAlreadyExists,Duplicate,DataIdentifierAlreadyExists,DuplicateRule,\
    DuplicateContent


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


    # register dataset
    def registerDataset(self,dsn,lfns=[],guids=[],sizes=[],checksums=[],lifetime=None,scope=None,metadata=None):
        presetScope = scope
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
            if presetScope is not None:
                scope = presetScope
            client.add_dataset(scope=scope, name=dsn, meta=metadata)
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
        if len(files) > 0:
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
    def registerDatasetLocation(self,dsn,rses,lifetime=None,owner=None,activity=None,scope=None,asynchronous=False,
                                grouping='DATASET',notify='N'):
        presetScope = scope
        if lifetime != None:
            lifetime = lifetime*24*60*60
        scope,dsn = self.extract_scope(dsn)    
        if presetScope is not None:
            scope = presetScope
        dids = []
        did = {'scope': scope, 'name': dsn}
        dids.append(did)
        # make location
        rses.sort()
        location = '|'.join(rses)
        # check if a replication rule already exists
        client = RucioClient()
        # owner
        if owner is None:
            owner = client.account
        for rule in client.list_did_rules(scope=scope, name=dsn):
            if (rule['rse_expression'] == location) and (rule['account'] == owner):
                return True
        try:
            client.add_replication_rule(dids=dids,copies=1,rse_expression=location,weight=None,
                                        lifetime=lifetime, grouping=grouping, account=owner,
                                        locked=False,activity=activity,notify=notify,
                                        ignore_availability=True,)
        except (Duplicate,DuplicateRule):
            pass
        return True



    # get user
    def getUser(self, client, dn):
        l = [i for i in client.list_accounts('user', dn)]
        if l != []:
            owner = l[0]['account']
            return owner
        return client.account



    # register dataset subscription
    def registerDatasetSubscription(self,dsn,rses,lifetime=None,owner=None,activity=None,dn=None,
                                    comment=None):
        if lifetime != None:
            lifetime = lifetime*24*60*60
        scope, dsn = self.extract_scope(dsn)
        dids = []
        did = {'scope': scope, 'name': dsn}
        dids.append(did)
        # make location
        rses.sort()
        location = '|'.join(rses)
        # check if a replication rule already exists
        client = RucioClient()
        # owner
        if owner is None:
            if dn is not None:
                owner = self.getUser(client, dn)
            else:
                owner = client.account
        for rule in client.list_did_rules(scope=scope, name=dsn):
            if (rule['rse_expression'] == location) and (rule['account'] == owner):
                return True
        try:
            client.add_replication_rule(dids=dids,copies=1,rse_expression=location,weight=None,
                                        lifetime=lifetime, grouping='DATASET', account=owner,
                                        locked=False,activity=activity,notify='C',
                                        ignore_availability=True,comment=comment)
        except (Duplicate,DuplicateRule):
            pass
        return True



    # convert file attribute
    def convFileAttr(self, tmpFile, scope):
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
        return file



    # register files in dataset
    def registerFilesInDataset(self,idMap,filesWoRSEs=None):
        # loop over all rse
        attachmentList = []
        for rse,tmpMap in idMap.iteritems():
            # loop over all datasets
            for datasetName,fileList in tmpMap.iteritems():
                # extract scope from dataset
                scope,dsn = self.extract_scope(datasetName)
                filesWithRSE = []
                filesWoRSE = []
                for tmpFile in fileList:
                    # convert file attribute
                    file = self.convFileAttr(tmpFile, scope)
                    # append files
                    if rse != None and (filesWoRSEs is None or file['name'] not in filesWoRSEs):
                        filesWithRSE.append(file)
                    else:
                        if 'pfn' in file:
                            del file['pfn']
                        filesWoRSE.append(file)
                # add attachment
                if len(filesWithRSE) > 0:
                    attachment = {'scope':scope,
                                  'name':dsn,
                                  'dids':filesWithRSE,
                                  'rse':rse}
                    attachmentList.append(attachment)
                if len(filesWoRSE) > 0:
                    attachment = {'scope':scope,
                                  'name':dsn,
                                  'dids':filesWoRSE}
                    attachmentList.append(attachment)
        # add files
        client = RucioClient()
        return client.add_files_to_datasets(attachmentList,ignore_duplicate=True)



    # register zip files
    def registerZipFiles(self,zipMap):
        # no zip files
        if len(zipMap) == 0:
            return
        client = RucioClient()
        # loop over all zip files
        for zipFileName, zipFileAttr in zipMap.iteritems():
            # convert file attribute
            zipFile = self.convFileAttr(zipFileAttr, zipFileAttr['scope'])
            # loop over all contents
            files = []
            for conFileAttr in zipFileAttr['files']:
                # get scope
                scope,dsn = self.extract_scope(conFileAttr['ds'])
                # convert file attribute
                conFile = self.convFileAttr(conFileAttr, scope)
                conFile['type'] = 'FILE'
                if 'pfn' in conFile:
                    del conFile['pfn']
                # append files
                files.append(conFile)
            # register zip file
            for rse in zipFileAttr['rse']:
                client.add_replicas(rse=rse, files=[zipFile])
            # add files
            client.add_files_to_archive(scope=zipFile['scope'],
                                        name=zipFile['name'],
                                        files=files)



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


    # list datasets
    def listDatasets(self,datasetName,old=False):
        result = {}
        # extract scope from dataset
        scope,dsn = self.extract_scope(datasetName)
        if dsn.endswith('/'):
            dsn = dsn[:-1]
            collection = 'container'
        else:
            collection = 'dataset'
        filters = {'name': dsn}
        try:
            # get dids
            client = RucioClient()
            for name in client.list_dids(scope, filters, type=collection):
                vuid = hashlib.md5(scope + ':' + name).hexdigest()
                vuid = '%s-%s-%s-%s-%s' % (vuid[0:8], vuid[8:12], vuid[12:16], vuid[16:20], vuid[20:32])
                duid = vuid
                # add /
                if datasetName.endswith('/') and not name.endswith('/'):
                    name += '/'
                if old or not ':' in datasetName:
                    keyName = name
                else:
                    keyName = str('%s:%s' % (scope, name))
                if keyName not in result:
                    result[keyName] = {'duid': duid, 'vuids': [vuid]}
            return result,''
        except:
            errType,errVale = sys.exc_info()[:2]
            return None,'%s %s' % (errType,errVale)



    # list datasets in container
    def listDatasetsInContainer(self,containerName):
        result = []
        # extract scope from dataset
        scope,cn = self.extract_scope(containerName)
        if cn.endswith('/'):
            cn = cn[:-1]
        try:
            # get dids
            client = RucioClient()
            for i in client.list_content(scope, cn):
                if i['type'] == 'DATASET':
                    result.append(str('%s:%s' % (i['scope'], i['name'])))
            return result,''
        except:
            errType,errVale = sys.exc_info()[:2]
            return None,'%s %s' % (errType,errVale)



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
            errType,errVale = sys.exc_info()[:2]
            return False,'%s %s' % (errType,errVale)
        return True,''



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


    # delete dataset
    def eraseDataset(self,dsn,scope=None):
        presetScope = scope
        # register dataset
        client = RucioClient()
        try:
            scope,dsn = self.extract_scope(dsn)
            if presetScope is not None:
                scope = presetScope
            client.set_metadata(scope=scope, name=dsn, key='lifetime', value=0.0001)
        except:
            errType,errVale = sys.exc_info()[:2]
            return False,'%s %s' % (errType,errVale)
        return True,''




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
    def listFileReplicas(self,scopes,lfns,rses=None):
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
                        tmpLFN = str(tmpDict['name'])
                        tmpRses = tmpDict['rses'].keys()
                        # RSE selection
                        if rses is not None:
                            newRSEs = []
                            for tmpRse in tmpRses:
                                if tmpRse in rses:
                                    newRSEs.append(tmpRse)
                            tmpRses = newRSEs
                        if len(tmpRses) > 0:
                            retVal[tmpLFN] = tmpRses
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
            dq2attrs['checksum'] = dq2attrs['chksum']
            dq2attrs['fsize'] = x['bytes']
            dq2attrs['filesize'] = dq2attrs['fsize']
            dq2attrs['scope'] = str(x['scope'])
            dq2attrs['events'] = str(x['events'])
            if long:
                dq2attrs['lumiblocknr'] = str(x['lumiblocknr'])
            guid = str('%s-%s-%s-%s-%s' % (x['guid'][0:8], x['guid'][8:12], x['guid'][12:16], x['guid'][16:20], x['guid'][20:32]))
            dq2attrs['guid'] = guid
            return_dict[tmpLFN] = dq2attrs
        return (return_dict, None)



    # get # of files in dataset
    def getNumberOfFiles(self,datasetName,presetScope=None):
        # extract scope from dataset
        scope,dsn = self.extract_scope(datasetName)
        if presetScope is not None:
            scope = presetScope
        client = RucioClient()
        nFiles = 0
        try:
            for x in client.list_files(scope, dsn, long=long):
                nFiles += 1
            return True,nFiles
        except DataIdentifierNotFound:
            return None,'dataset not found'
        except:
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            return False,errMsg



    # get dataset size
    def getDatasetSize(self,datasetName):
        # extract scope from dataset
        scope,dsn = self.extract_scope(datasetName)
        client = RucioClient()
        tSize = 0
        try:
            for x in client.list_files(scope, dsn, long=long):
                tSize += x['bytes']
            return True,tSize
        except DataIdentifierNotFound:
            return None,'dataset not found'
        except:
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            return False,errMsg



    # delete dataset replicas
    def deleteDatasetReplicas(self,datasetName,locations):
        # extract scope from dataset
        scope,dsn = self.extract_scope(datasetName)
        client = RucioClient()
        try:
            for rule in self.client.list_did_rules(scope, dsn):
                if rule['account'] != self.client.account:
                    continue
                if rule['rse_expression'] in locations:
                    client.delete_replication_rule(rule['id'])
        except DataIdentifierNotFound:
            pass
        except:
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            return False,errMsg
        return True,''



    # list active subscriptions
    def deleteDatasetReplicas(self,datasetName):
        # extract scope from dataset
        scope,dsn = self.extract_scope(datasetName)
        client = RucioClient()
        rse_expressions = []
        list_rses = []
        result = []
        try:
            for rule in self.client.list_did_rules(scope, dsn):
                if rule['state'] != 'OK' and rule['rse_expression'] not in rse_expressions:
                    rse_expressions.append(rule['rse_expression'])
            for rse_expression in rse_expressions:
                for rse in client.list_rses(rse_expression):
                    if rse not in list_rses:
                        list_rses.append(rse['rse'])
            result = list_rses
        except DataIdentifierNotFound:
            pass
        except:
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            return False,errMsg
        return True,result



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



    # list datasets with GUIDs
    def listDatasetsByGUIDs(self,guids):
        client = RucioClient()
        result = {}
        for guid in guids:
            datasets = [str('%s:%s' % (i['scope'], i['name'])) for i in client.get_dataset_by_guid(guid)]
            result[guid] = datasets
        return result



    # finger
    def finger(self, userName):
        try:
            # get rucio API
            client = RucioClient()
            userInfo = None
            retVal = False
            for i in client.list_accounts(account_type='USER',identity=userName):
                userInfo = {'nickname':i['account'],
                            'email':i['email']}
                break
            if userInfo == None:
                # remove /CN=\d
                userName = re.sub('(/CN=\d+)+$','',userName)
                for i in client.list_accounts(account_type='USER',identity=userName):
                    userInfo = {'nickname':i['account'],
                                'email':i['email']}
                    break
            if userInfo is not None:
                retVal = True
        except:
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            userInfo = errMsg
        return retVal,userInfo



    # register container
    def registerContainer(self,cname,datasets=[],presetScope=None):
        if cname.endswith('/'):
            cname = cname[:-1]
        # register container
        client = RucioClient()
        try:
            scope,dsn = self.extract_scope(cname)
            if presetScope is not None:
                scope = presetScope
            client.add_container(scope=scope, name=cname)
        except DataIdentifierAlreadyExists:
            pass
        # add files
        if len(datasets) > 0:
            try:
                dsns = []
                for ds in datasets:
                    ds_scope, ds_name = self.extract_scope(ds)
                    if ds_scope:
                        dsn = {'scope': ds_scope, 'name': ds_name}
                    else:
                        dsn = {'scope': scope, 'name': ds}
                    dsns.append(dsn)
                client.add_datasets_to_container(scope=scope, name=cname, dsns=dsns)
            except DuplicateContent:
                for ds in dsns:
                    try:
                        client.add_datasets_to_container(scope=scope, name=cname, dsns=[ds])
                    except DuplicateContent:
                        pass
        return True


    
    # get a parsed certificate DN
    def parse_dn(self,tmpDN):
        if tmpDN is not None:
            tmpDN = re.sub('/CN=limited proxy','',tmpDN)
            tmpDN = re.sub('(/CN=proxy)+$', '', tmpDN)
            #tmpDN = re.sub('(/CN=\d+)+$', '', tmpDN)
        return tmpDN
        

# instantiate
rucioAPI = RucioAPI()
del RucioAPI

