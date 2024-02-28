"""
provide primitive methods for DDM

"""

import hashlib
import re
import sys
import traceback

from rucio.client import Client as RucioClient
from rucio.common.exception import (
    DataIdentifierAlreadyExists,
    DataIdentifierNotFound,
    Duplicate,
    DuplicateContent,
    DuplicateRule,
    FileAlreadyExists,
    UnsupportedOperation,
)

from pandaserver.srvcore import CoreUtils


# rucio
class RucioAPI:
    # constructor
    def __init__(self):
        pass

    # extract scope
    def extract_scope(self, dsn):
        if ":" in dsn:
            return dsn.split(":")[:2]
        scope = dsn.split(".")[0]
        if dsn.startswith("user") or dsn.startswith("group"):
            scope = ".".join(dsn.split(".")[0:2])
        return scope, dsn

    # register dataset
    def registerDataset(
        self,
        dsn,
        lfns=None,
        guids=None,
        sizes=None,
        checksums=None,
        lifetime=None,
        scope=None,
        metadata=None,
    ):
        if lfns is None:
            lfns = []
        if guids is None:
            guids = []
        if sizes is None:
            sizes = []
        if checksums is None:
            checksums = []
        presetScope = scope
        files = []
        for lfn, guid, size, checksum in zip(lfns, guids, sizes, checksums):
            if lfn.find(":") > -1:
                s, lfn = lfn.split(":")[0], lfn.split(":")[1]
            else:
                s = scope
            file = {"scope": s, "name": lfn, "bytes": size, "meta": {"guid": guid}}
            if checksum.startswith("md5:"):
                file["md5"] = checksum[4:]
            elif checksum.startswith("ad:"):
                file["adler32"] = checksum[3:]
            files.append(file)
        # register dataset
        client = RucioClient()
        try:
            scope, dsn = self.extract_scope(dsn)
            if presetScope is not None:
                scope = presetScope
            client.add_dataset(scope=scope, name=dsn, meta=metadata)
            if lifetime is not None:
                client.set_metadata(scope, dsn, key="lifetime", value=lifetime * 86400)
        except DataIdentifierAlreadyExists:
            pass
        # open dataset just in case
        try:
            client.set_status(scope, dsn, open=True)
        except Exception:
            pass
        # add files
        if len(files) > 0:
            iFiles = 0
            nFiles = 1000
            while iFiles < len(files):
                tmpFiles = files[iFiles : iFiles + nFiles]
                try:
                    client.add_files_to_dataset(scope=scope, name=dsn, files=tmpFiles, rse=None)
                except FileAlreadyExists:
                    for f in tmpFiles:
                        try:
                            client.add_files_to_dataset(scope=scope, name=dsn, files=[f], rse=None)
                        except FileAlreadyExists:
                            pass
                iFiles += nFiles
        vuid = hashlib.md5((scope + ":" + dsn).encode()).hexdigest()
        vuid = f"{vuid[0:8]}-{vuid[8:12]}-{vuid[12:16]}-{vuid[16:20]}-{vuid[20:32]}"
        duid = vuid
        return {"duid": duid, "version": 1, "vuid": vuid}

    # register dataset location
    def registerDatasetLocation(
        self,
        dsn,
        rses,
        lifetime=None,
        owner=None,
        activity=None,
        scope=None,
        asynchronous=False,
        grouping="DATASET",
        notify="N",
    ):
        if grouping is None:
            grouping = "DATASET"
        presetScope = scope
        if lifetime is not None:
            lifetime = lifetime * 24 * 60 * 60
        scope, dsn = self.extract_scope(dsn)
        if presetScope is not None:
            scope = presetScope
        dids = []
        did = {"scope": scope, "name": dsn}
        dids.append(did)
        # make location
        rses.sort()
        location = "|".join(rses)
        # check if a replication rule already exists
        client = RucioClient()
        # owner
        if owner is None:
            owner = client.account
        for rule in client.list_did_rules(scope=scope, name=dsn):
            if (rule["rse_expression"] == location) and (rule["account"] == owner):
                return True
        try:
            client.add_replication_rule(
                dids=dids,
                copies=1,
                rse_expression=location,
                weight=None,
                lifetime=lifetime,
                grouping=grouping,
                account=owner,
                locked=False,
                activity=activity,
                notify=notify,
                ignore_availability=True,
            )
        except (Duplicate, DuplicateRule):
            pass
        return True

    # get user
    def getUser(self, client, dn):
        tmp_list = [i for i in client.list_accounts("user", dn)]
        if tmp_list != []:
            owner = l[0]["account"]
            return owner
        return client.account

    # register dataset subscription
    def registerDatasetSubscription(self, dsn, rses, lifetime=None, owner=None, activity=None, dn=None, comment=None):
        if lifetime is not None:
            lifetime = lifetime * 24 * 60 * 60
        scope, dsn = self.extract_scope(dsn)
        dids = []
        did = {"scope": scope, "name": dsn}
        dids.append(did)
        # make location
        rses.sort()
        location = "|".join(rses)
        # check if a replication rule already exists
        client = RucioClient()
        # owner
        if owner is None:
            if dn is not None:
                owner = self.getUser(client, dn)
            else:
                owner = client.account
        for rule in client.list_did_rules(scope=scope, name=dsn):
            if (rule["rse_expression"] == location) and (rule["account"] == owner):
                return True
        try:
            client.add_replication_rule(
                dids=dids,
                copies=1,
                rse_expression=location,
                weight=None,
                lifetime=lifetime,
                grouping="DATASET",
                account=owner,
                locked=False,
                activity=activity,
                notify="C",
                ignore_availability=True,
                comment=comment,
            )
        except (Duplicate, DuplicateRule):
            pass
        return True

    # convert file attribute
    def convFileAttr(self, tmpFile, scope):
        # extract scope from LFN if available
        if "name" in tmpFile:
            lfn = tmpFile["name"]
        else:
            lfn = tmpFile["lfn"]
        if ":" in lfn:
            s, lfn = lfn.split(":")
        else:
            s = scope
        # set metadata
        meta = {}
        if "guid" in tmpFile:
            meta["guid"] = tmpFile["guid"]
        if "events" in tmpFile:
            meta["events"] = tmpFile["events"]
        if "lumiblocknr" in tmpFile:
            meta["lumiblocknr"] = tmpFile["lumiblocknr"]
        if "panda_id" in tmpFile:
            meta["panda_id"] = tmpFile["panda_id"]
        if "campaign" in tmpFile:
            meta["campaign"] = tmpFile["campaign"]
        if "task_id" in tmpFile:
            meta["task_id"] = tmpFile["task_id"]
        if "bytes" in tmpFile:
            fsize = tmpFile["bytes"]
        else:
            fsize = tmpFile["size"]
        # set mandatory fields
        file = {"scope": s, "name": lfn, "bytes": fsize, "meta": meta}
        if "checksum" in tmpFile:
            checksum = tmpFile["checksum"]
            if checksum.startswith("md5:"):
                file["md5"] = checksum[4:]
            elif checksum.startswith("ad:"):
                file["adler32"] = checksum[3:]
        if "surl" in tmpFile:
            file["pfn"] = tmpFile["surl"]
        return file

    # register files in dataset
    def registerFilesInDataset(self, idMap, filesWoRSEs=None):
        # loop over all rse
        attachmentList = []
        for rse in idMap:
            tmpMap = idMap[rse]
            # loop over all datasets
            for datasetName in tmpMap:
                fileList = tmpMap[datasetName]
                # extract scope from dataset
                scope, dsn = self.extract_scope(datasetName)
                filesWithRSE = []
                filesWoRSE = []
                for tmpFile in fileList:
                    # convert file attribute
                    file = self.convFileAttr(tmpFile, scope)
                    # append files
                    if rse is not None and (filesWoRSEs is None or file["name"] not in filesWoRSEs):
                        filesWithRSE.append(file)
                    else:
                        if "pfn" in file:
                            del file["pfn"]
                        filesWoRSE.append(file)
                # add attachment
                if len(filesWithRSE) > 0:
                    nFiles = 100
                    iFiles = 0
                    while iFiles < len(filesWithRSE):
                        attachment = {
                            "scope": scope,
                            "name": dsn,
                            "dids": filesWithRSE[iFiles : iFiles + nFiles],
                            "rse": rse,
                        }
                        attachmentList.append(attachment)
                        iFiles += nFiles
                if len(filesWoRSE) > 0:
                    nFiles = 100
                    iFiles = 0
                    while iFiles < len(filesWoRSE):
                        attachment = {
                            "scope": scope,
                            "name": dsn,
                            "dids": filesWoRSE[iFiles : iFiles + nFiles],
                        }
                        attachmentList.append(attachment)
                        iFiles += nFiles
        # add files
        client = RucioClient()
        client.add_files_to_datasets(attachmentList, ignore_duplicate=True)
        return True

    # register zip files
    def registerZipFiles(self, zipMap):
        # no zip files
        if len(zipMap) == 0:
            return
        client = RucioClient()
        # loop over all zip files
        for zipFileName in zipMap:
            zipFileAttr = zipMap[zipFileName]
            # convert file attribute
            zipFile = self.convFileAttr(zipFileAttr, zipFileAttr["scope"])
            # loop over all contents
            files = []
            for conFileAttr in zipFileAttr["files"]:
                # get scope
                scope, dsn = self.extract_scope(conFileAttr["ds"])
                # convert file attribute
                conFile = self.convFileAttr(conFileAttr, scope)
                conFile["type"] = "FILE"
                if "pfn" in conFile:
                    del conFile["pfn"]
                # append files
                files.append(conFile)
            # register zip file
            for rse in zipFileAttr["rse"]:
                client.add_replicas(rse=rse, files=[zipFile])
            # add files
            nFiles = 100
            iFiles = 0
            while iFiles < len(files):
                client.add_files_to_archive(
                    scope=zipFile["scope"],
                    name=zipFile["name"],
                    files=files[iFiles : iFiles + nFiles],
                )
                iFiles += nFiles

    # list datasets
    def listDatasets(self, datasetName, old=False):
        result = {}
        # extract scope from dataset
        scope, dsn = self.extract_scope(datasetName)
        if dsn.endswith("/"):
            dsn = dsn[:-1]
            collection = "container"
        else:
            collection = "dataset"
        filters = {"name": dsn}
        try:
            # get dids
            client = RucioClient()
            for name in client.list_dids(scope, filters, collection):
                vuid = hashlib.md5((scope + ":" + name).encode()).hexdigest()
                vuid = f"{vuid[0:8]}-{vuid[8:12]}-{vuid[12:16]}-{vuid[16:20]}-{vuid[20:32]}"
                duid = vuid
                # add /
                if datasetName.endswith("/") and not name.endswith("/"):
                    name += "/"
                if old or ":" not in datasetName:
                    keyName = name
                else:
                    keyName = str(f"{scope}:{name}")
                if keyName not in result:
                    result[keyName] = {"duid": duid, "vuids": [vuid]}
            return result, ""
        except Exception as e:
            return None, f"{str(e)} {traceback.format_exc()}"

    # list datasets in container
    def listDatasetsInContainer(self, containerName):
        result = []
        # extract scope from dataset
        scope, cn = self.extract_scope(containerName)
        if cn.endswith("/"):
            cn = cn[:-1]
        try:
            # get dids
            client = RucioClient()
            for i in client.list_content(scope, cn):
                if i["type"] == "DATASET":
                    result.append(str(f"{i['scope']}:{i['name']}"))
            return result, ""
        except Exception:
            errType, errVale = sys.exc_info()[:2]
            return None, f"{errType} {errVale}"

    # list dataset replicas
    def listDatasetReplicas(self, datasetName):
        retMap = {}
        # extract scope from dataset
        scope, dsn = self.extract_scope(datasetName)
        try:
            # get replicas
            client = RucioClient()
            itr = client.list_dataset_replicas(scope, dsn)
            for item in itr:
                rse = item["rse"]
                retMap[rse] = [
                    {
                        "total": item["length"],
                        "found": item["available_length"],
                        "immutable": 1,
                    }
                ]
            return 0, retMap
        except Exception:
            errType, errVale = sys.exc_info()[:2]
            return 1, f"{errType} {errVale}"

    # set metadata
    def setMetaData(self, dsn, metadata=None):
        # register dataset
        client = RucioClient()
        try:
            scope, dsn = self.extract_scope(dsn)
            for tmpKey in metadata:
                tmpValue = metadata[tmpKey]
                client.set_metadata(scope, dsn, key=tmpKey, value=tmpValue)
        except Exception:
            errType, errVale = sys.exc_info()[:2]
            return False, f"{errType} {errVale}"
        return True, ""

    # get metadata
    def getMetaData(self, dsn):
        # register dataset
        client = RucioClient()
        try:
            scope, dsn = self.extract_scope(dsn)
            return True, client.get_metadata(scope, dsn)
        except DataIdentifierNotFound:
            return True, None
        except Exception:
            errType, errVale = sys.exc_info()[:2]
            return False, f"{errType} {errVale}"

    # delete dataset
    def eraseDataset(self, dsn, scope=None, grace_period=None):
        presetScope = scope
        # register dataset
        client = RucioClient()
        try:
            scope, dsn = self.extract_scope(dsn)
            if presetScope is not None:
                scope = presetScope
            if grace_period is not None:
                value = grace_period * 60 * 60
            else:
                value = 0.0001
            client.set_metadata(scope=scope, name=dsn, key="lifetime", value=value)
        except DataIdentifierNotFound:
            pass
        except Exception as e:
            return False, f"{str(e)}"
        return True, ""

    # close dataset
    def closeDataset(self, dsn):
        # register dataset
        client = RucioClient()
        try:
            scope, dsn = self.extract_scope(dsn)
            client.set_status(scope, dsn, open=False)
        except (UnsupportedOperation, DataIdentifierNotFound):
            pass
        return True

    # list file replicas
    def listFileReplicas(self, scopes, lfns, rses=None):
        try:
            client = RucioClient()
            dids = []
            iGUID = 0
            nGUID = 1000
            retVal = {}
            for scope, lfn in zip(scopes, lfns):
                iGUID += 1
                dids.append({"scope": scope, "name": lfn})
                if len(dids) % nGUID == 0 or iGUID == len(lfns):
                    for tmpDict in client.list_replicas(dids):
                        tmpLFN = str(tmpDict["name"])
                        tmpRses = list(tmpDict["rses"])
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
            return True, retVal
        except Exception:
            errType, errVale = sys.exc_info()[:2]
            return False, f"{errType} {errVale}"

    # get zip files
    def getZipFiles(self, dids, rses):
        try:
            client = RucioClient()
            data = []
            iGUID = 0
            nGUID = 1000
            retVal = {}
            for did in dids:
                iGUID += 1
                scope, lfn = did.split(":")
                data.append({"scope": scope, "name": lfn})
                if len(data) % nGUID == 0 or iGUID == len(dids):
                    for tmpDict in client.list_replicas(data):
                        tmpScope = str(tmpDict["scope"])
                        tmpLFN = str(tmpDict["name"])
                        tmpDID = f"{tmpScope}:{tmpLFN}"
                        # RSE selection
                        for pfn in tmpDict["pfns"]:
                            pfnData = tmpDict["pfns"][pfn]
                            if (rses is None or pfnData["rse"] in rses) and pfnData["domain"] == "zip":
                                zipFileName = pfn.split("/")[-1]
                                zipFileName = re.sub("\?.+$", "", zipFileName)
                                retVal[tmpDID] = client.get_metadata(tmpScope, zipFileName)
                                break
                    data = []
            return True, retVal
        except Exception:
            errType, errVale = sys.exc_info()[:2]
            return False, f"{errType} {errVale}"

    # list files in dataset
    def listFilesInDataset(self, datasetName, long=False, fileList=None):
        # extract scope from dataset
        scope, dsn = self.extract_scope(datasetName)
        if dsn.endswith("/"):
            dsn = dsn[:-1]
        client = RucioClient()
        return_dict = {}
        for x in client.list_files(scope, dsn, long=long):
            tmpLFN = str(x["name"])
            if fileList is not None:
                genLFN = re.sub("\.\d+$", "", tmpLFN)
                if tmpLFN not in fileList and genLFN not in fileList:
                    continue
            dq2attrs = {}
            dq2attrs["chksum"] = "ad:" + str(x["adler32"])
            dq2attrs["md5sum"] = dq2attrs["chksum"]
            dq2attrs["checksum"] = dq2attrs["chksum"]
            dq2attrs["fsize"] = x["bytes"]
            dq2attrs["filesize"] = dq2attrs["fsize"]
            dq2attrs["scope"] = str(x["scope"])
            dq2attrs["events"] = str(x["events"])
            if long:
                dq2attrs["lumiblocknr"] = str(x["lumiblocknr"])
            guid = str(f"{x['guid'][0:8]}-{x['guid'][8:12]}-{x['guid'][12:16]}-{x['guid'][16:20]}-{x['guid'][20:32]}")
            dq2attrs["guid"] = guid
            return_dict[tmpLFN] = dq2attrs
        return (return_dict, None)

    # get # of files in dataset
    def getNumberOfFiles(self, datasetName, presetScope=None):
        # extract scope from dataset
        scope, dsn = self.extract_scope(datasetName)
        if presetScope is not None:
            scope = presetScope
        client = RucioClient()
        nFiles = 0
        try:
            for x in client.list_files(scope, dsn):
                nFiles += 1
            return True, nFiles
        except DataIdentifierNotFound:
            return None, "dataset not found"
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            errMsg = f"{errtype.__name__} {errvalue}"
            return False, errMsg

    # get dataset size
    def getDatasetSize(self, datasetName):
        if datasetName.endswith("/"):
            datasetName = datasetName[:-1]
        # extract scope from dataset
        scope, dsn = self.extract_scope(datasetName)
        client = RucioClient()
        tSize = 0
        try:
            for x in client.list_files(scope, dsn):
                tSize += x["bytes"]
            return True, tSize
        except DataIdentifierNotFound:
            return None, "dataset not found"
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            errMsg = f"{errtype.__name__} {errvalue}"
            return False, errMsg

    # register files
    def registerFiles(self, files, rse):
        client = RucioClient()
        try:
            # add replicas
            client.add_replicas(files=files, rse=rse)
        except FileAlreadyExists:
            pass
        try:
            # add rule
            client.add_replication_rule(files, copies=1, rse_expression=rse)
        except DuplicateRule:
            pass

    # delete files from dataset
    def deleteFilesFromDataset(self, datasetName, files):
        # extract scope from dataset
        scope, dsn = self.extract_scope(datasetName)
        client = RucioClient()
        try:
            # delete files
            client.detach_dids(scope=scope, name=dsn, dids=files)
        except DataIdentifierNotFound:
            pass

    # list datasets with GUIDs
    def listDatasetsByGUIDs(self, guids):
        client = RucioClient()
        result = {}
        for guid in guids:
            datasets = [str(f"{i['scope']}:{i['name']}") for i in client.get_dataset_by_guid(guid)]
            result[guid] = datasets
        return result

    # finger
    def finger(self, dn):
        try:
            # get rucio API
            client = RucioClient()
            userInfo = None
            retVal = False
            x509_user_name = CoreUtils.get_bare_dn(dn)
            oidc_user_name = CoreUtils.get_id_from_dn(dn)
            if oidc_user_name == x509_user_name:
                oidc_user_name = None
            else:
                x509_user_name = None
            for accType in ["USER", "GROUP"]:
                if x509_user_name is not None:
                    userName = x509_user_name
                    for i in client.list_accounts(account_type=accType, identity=userName):
                        userInfo = {"nickname": i["account"], "email": i["email"]}
                        break
                    if userInfo is None:
                        userName = CoreUtils.get_bare_dn(dn, keep_digits=False)
                        for i in client.list_accounts(account_type=accType, identity=userName):
                            userInfo = {"nickname": i["account"], "email": i["email"]}
                            break
                else:
                    userName = oidc_user_name
                try:
                    if userInfo is None:
                        i = client.get_account(userName)
                        userInfo = {"nickname": i["account"], "email": i["email"]}
                except Exception:
                    pass
                if userInfo is not None:
                    retVal = True
                    break
        except Exception as e:
            errMsg = f"{str(e)}"
            userInfo = errMsg
        return retVal, userInfo

    # register container
    def registerContainer(self, cname, datasets=[], presetScope=None):
        if cname.endswith("/"):
            cname = cname[:-1]
        # register container
        client = RucioClient()
        try:
            scope, dsn = self.extract_scope(cname)
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
                        dsn = {"scope": ds_scope, "name": ds_name}
                    else:
                        dsn = {"scope": scope, "name": ds}
                    dsns.append(dsn)
                client.add_datasets_to_container(scope=scope, name=cname, dsns=dsns)
            except DuplicateContent:
                for ds in dsns:
                    try:
                        client.add_datasets_to_container(scope=scope, name=cname, dsns=[ds])
                    except DuplicateContent:
                        pass
        return True


# instantiate
rucioAPI = RucioAPI()
del RucioAPI
