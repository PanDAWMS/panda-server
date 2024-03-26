"""
Module to provide primitive methods for DDM

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
    """
    A class to interact with Rucio API
    """
    # constructor
    def __init__(self):
        """
        Initialize RucioAPI instance
        """
        pass

    # extract scope
    def extract_scope(self, dataset_name: str) -> tuple:
        """
        Extract scope from a given dataset name

        Parameters:
        dataset_name (str): Dataset name

        Returns:
        tuple: A tuple containing scope and dataset name
        """
        if ":" in dataset_name:
            return dataset_name.split(":")[:2]
        scope = dataset_name.split(".")[0]
        if dataset_name.startswith("user") or dataset_name.startswith("group"):
            scope = ".".join(dataset_name.split(".")[0:2])
        return scope, dataset_name

    # register dataset
    def register_dataset(
        self,
        dataset_name: str,
        lfns: list = None,
        guids: list = None,
        sizes: list = None,
        checksums: list = None,
        lifetime: int = None,
        scope: str = None,
        metadata: dict = None,
    ) -> dict:
        """
        Register a dataset in Rucio

        Parameters:
        dataset_name (str): Dataset name
        lfns (list, optional): List of logical file names. Defaults to None.
        guids (list, optional): List of GUIDs. Defaults to None.
        sizes (list, optional): List of file sizes. Defaults to None.
        checksums (list, optional): List of checksums. Defaults to None.
        lifetime (int, optional): Lifetime of the dataset in seconds. Defaults to None.
        scope (str, optional): Scope of the dataset. Defaults to None.
        metadata (dict, optional): Metadata of the dataset. Defaults to None.

        Returns:
        dict: A dictionary containing duid, version and vuid of the dataset
        """
        if lfns is None:
            lfns = []
        if guids is None:
            guids = []
        if sizes is None:
            sizes = []
        if checksums is None:
            checksums = []
        preset_scope = scope
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
            scope, dataset_name = self.extract_scope(dataset_name)
            if preset_scope is not None:
                scope = preset_scope
            client.add_dataset(scope=scope, name=dataset_name, meta=metadata)
            if lifetime is not None:
                client.set_metadata(scope, dataset_name, key="lifetime", value=lifetime * 86400)
        except DataIdentifierAlreadyExists:
            pass
        # open dataset just in case
        try:
            client.set_status(scope, dataset_name, open=True)
        except Exception:
            pass
        # add files
        if len(files) > 0:
            i_files = 0
            n_files = 1000
            while i_files < len(files):
                tmp_files = files[i_files : i_files + n_files]
                try:
                    client.add_files_to_dataset(scope=scope, name=dataset_name, files=tmp_files, rse=None)
                except FileAlreadyExists:
                    for f in tmp_files:
                        try:
                            client.add_files_to_dataset(scope=scope, name=dataset_name, files=[f], rse=None)
                        except FileAlreadyExists:
                            pass
                i_files += n_files
        vuid = hashlib.md5((scope + ":" + dataset_name).encode()).hexdigest()
        vuid = f"{vuid[0:8]}-{vuid[8:12]}-{vuid[12:16]}-{vuid[16:20]}-{vuid[20:32]}"
        duid = vuid
        return {"duid": duid, "version": 1, "vuid": vuid}

    def register_dataset_location(
        self,
        dataset_name: str,
        rses: list,
        lifetime: int = None,
        owner: str = None,
        activity: str = None,
        scope: str = None,
        asynchronous: bool = False,
        grouping: str = "DATASET",
        notify: str = "N",
    ) -> bool:
        """
        Register a dataset location in Rucio

        Parameters:
        dataset_name (str): Dataset name
        rses (list): List of RSEs
        lifetime (int, optional): Lifetime of the dataset in seconds. Defaults to None.
        owner (str, optional): Owner of the dataset. Defaults to None.
        activity (str, optional): Activity associated with the dataset. Defaults to None.
        scope (str, optional): Scope of the dataset. Defaults to None.
        asynchronous (bool, optional): Flag to indicate if the operation is asynchronous. Defaults to False.
        grouping (str, optional): Grouping of the dataset. Defaults to "DATASET".
        notify (str, optional): Notification option. Defaults to "N".

        Returns:
        bool: True if the operation is successful, False otherwise
        """
        grouping = "DATASET" if grouping is None else grouping
        preset_scope = scope
        lifetime = lifetime * 24 * 60 * 60 if lifetime else None
        scope, dataset_name = self.extract_scope(dataset_name)
        scope = preset_scope if preset_scope else scope
        dids = []
        did = {"scope": scope, "name": dataset_name}
        dids.append(did)
        # make location
        rses.sort()
        location = "|".join(rses)
        # check if a replication rule already exists
        client = RucioClient()
        # owner
        owner = client.account if owner is None else owner
        for rule in client.list_did_rules(scope=scope, name=dataset_name):
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
    def getUser(self, client, dn: str) -> str:
        """
        This method retrieves the account name associated with a given distinguished name (dn) from the Rucio client.
        If no account is found, it returns the default account of the Rucio client.

        Parameters:
        client (RucioClient): The Rucio client instance to interact with the Rucio server.
        dn (str): The distinguished name (dn) for which the associated account is to be retrieved.

        Returns:
        str: The account name associated with the given dn. If no account is found, it returns the default account of the Rucio client.
        """
        tmp_list = [i for i in client.list_accounts("user", dn)]
        if tmp_list != []:
            owner = l[0]["account"]
            return owner
        return client.account

    # register dataset subscription
    def register_dataset_subscription(self, dataset_name: str, rses: list, lifetime: int = None, owner: str = None,
                                    activity: str = None, dn: str = None, comment: str = None) -> bool:
        """
        Register a dataset subscription in Rucio.

        This method registers a dataset subscription in Rucio by creating a replication rule for the dataset.
        If a replication rule already exists for the dataset, the method simply returns True.

        Parameters:
        dataset_name (str): The name of the dataset.
        rses (list): A list of RSEs where the dataset should be replicated.
        lifetime (int, optional): The lifetime of the replication rule in seconds. Defaults to None.
        owner (str, optional): The owner of the replication rule. Defaults to None.
        activity (str, optional): The activity associated with the replication rule. Defaults to None.
        dn (str, optional): The distinguished name of the user. Defaults to None.
        comment (str, optional): A comment to be associated with the replication rule. Defaults to None.

        Returns:
        bool: True if the operation is successful, False otherwise.
        """
        lifetime = lifetime * 24 * 60 * 60 if lifetime else lifetime
        scope, dataset_name = self.extract_scope(dataset_name)
        dids = []
        did = {"scope": scope, "name": dataset_name}
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
        for rule in client.list_did_rules(scope=scope, name=dataset_name):
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
    def conv_file_attr(self, tmp_file: dict, scope: str) -> dict:
        """
        Convert file attribute to a dictionary

        Parameters:
        tmp_file (dict): File attribute
        scope (str): Scope of the file

        Returns:
        dict: A dictionary containing file attributes
        """
        lfn = tmpFile.get("name", tmpFile.get("lfn"))
        s, lfn = lfn.split(":") if ":" in lfn else (scope, lfn)
        # set metadata
        meta_keys = ["guid", "events", "lumiblocknr", "panda_id", "campaign", "task_id"]
        meta = {key: tmpFile[key] for key in meta_keys if key in tmpFile}
        fsize = tmpFile.get("bytes", tmpFile.get("size"))
        file = {"scope": s, "name": lfn, "bytes": fsize, "meta": meta}
        if "checksum" in tmp_file:
            checksum = tmp_file["checksum"]
            if checksum.startswith("md5:"):
                file["md5"] = checksum[4:]
            elif checksum.startswith("ad:"):
                file["adler32"] = checksum[3:]
        if "surl" in tmp_file:
            file["pfn"] = tmp_file["surl"]
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
                scope, dataset_name = self.extract_scope(datasetName)
                filesWithRSE = []
                filesWoRSE = []
                for tmpFile in fileList:
                    # convert file attribute
                    file = self.conv_file_attr(tmpFile, scope)
                    # append files
                    if rse is not None and (filesWoRSEs is None or file["name"] not in filesWoRSEs):
                        filesWithRSE.append(file)
                    else:
                        if "pfn" in file:
                            del file["pfn"]
                        filesWoRSE.append(file)
                # add attachment
                if len(filesWithRSE) > 0:
                    n_files = 100
                    i_files = 0
                    while i_files < len(filesWithRSE):
                        attachment = {
                            "scope": scope,
                            "name": dataset_name,
                            "dids": filesWithRSE[i_files : i_files + n_files],
                            "rse": rse,
                        }
                        attachmentList.append(attachment)
                        i_files += n_files
                if len(filesWoRSE) > 0:
                    n_files = 100
                    i_files = 0
                    while i_files < len(filesWoRSE):
                        attachment = {
                            "scope": scope,
                            "name": dataset_name,
                            "dids": filesWoRSE[i_files : i_files + n_files],
                        }
                        attachmentList.append(attachment)
                        i_files += n_files
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
            zipFile = self.conv_file_attr(zipFileAttr, zipFileAttr["scope"])
            # loop over all contents
            files = []
            for conFileAttr in zipFileAttr["files"]:
                # get scope
                scope, dataset_name = self.extract_scope(conFileAttr["ds"])
                # convert file attribute
                conFile = self.conv_file_attr(conFileAttr, scope)
                conFile["type"] = "FILE"
                if "pfn" in conFile:
                    del conFile["pfn"]
                # append files
                files.append(conFile)
            # register zip file
            for rse in zipFileAttr["rse"]:
                client.add_replicas(rse=rse, files=[zipFile])
            # add files
            n_files = 100
            i_files = 0
            while i_files < len(files):
                client.add_files_to_archive(
                    scope=zipFile["scope"],
                    name=zipFile["name"],
                    files=files[i_files : i_files + n_files],
                )
                i_files += n_files

    # list datasets
    def listDatasets(self, datasetName, old=False):
        result = {}
        # extract scope from dataset
        scope, dataset_name = self.extract_scope(datasetName)
        if dataset_name.endswith("/"):
            dataset_name = dataset_name[:-1]
            collection = "container"
        else:
            collection = "dataset"
        filters = {"name": dataset_name}
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
        scope, dataset_name = self.extract_scope(datasetName)
        try:
            # get replicas
            client = RucioClient()
            itr = client.list_dataset_replicas(scope, dataset_name)
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
    def setMetaData(self, dataset_name, metadata=None):
        # register dataset
        client = RucioClient()
        try:
            scope, dataset_name = self.extract_scope(dataset_name)
            for tmpKey in metadata:
                tmpValue = metadata[tmpKey]
                client.set_metadata(scope, dataset_name, key=tmpKey, value=tmpValue)
        except Exception:
            errType, errVale = sys.exc_info()[:2]
            return False, f"{errType} {errVale}"
        return True, ""

    # get metadata
    def getMetaData(self, dataset_name):
        # register dataset
        client = RucioClient()
        try:
            scope, dataset_name = self.extract_scope(dataset_name)
            return True, client.get_metadata(scope, dataset_name)
        except DataIdentifierNotFound:
            return True, None
        except Exception:
            errType, errVale = sys.exc_info()[:2]
            return False, f"{errType} {errVale}"

    # delete dataset
    def eraseDataset(self, dataset_name, scope=None, grace_period=None):
        preset_scope = scope
        # register dataset
        client = RucioClient()
        try:
            scope, dataset_name = self.extract_scope(dataset_name)
            if preset_scope is not None:
                scope = preset_scope
            if grace_period is not None:
                value = grace_period * 60 * 60
            else:
                value = 0.0001
            client.set_metadata(scope=scope, name=dataset_name, key="lifetime", value=value)
        except DataIdentifierNotFound:
            pass
        except Exception as e:
            return False, f"{str(e)}"
        return True, ""

    # close dataset
    def closeDataset(self, dataset_name):
        # register dataset
        client = RucioClient()
        try:
            scope, dataset_name = self.extract_scope(dataset_name)
            client.set_status(scope, dataset_name, open=False)
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
        scope, dataset_name = self.extract_scope(datasetName)
        if dataset_name.endswith("/"):
            dataset_name = dataset_name[:-1]
        client = RucioClient()
        return_dict = {}
        for x in client.list_files(scope, dataset_name, long=long):
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
    def getNumberOfFiles(self, datasetName, preset_scope=None):
        # extract scope from dataset
        scope, dataset_name = self.extract_scope(datasetName)
        if preset_scope is not None:
            scope = preset_scope
        client = RucioClient()
        n_files = 0
        try:
            for x in client.list_files(scope, dataset_name):
                n_files += 1
            return True, n_files
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
        scope, dataset_name = self.extract_scope(datasetName)
        client = RucioClient()
        tSize = 0
        try:
            for x in client.list_files(scope, dataset_name):
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
        scope, dataset_name = self.extract_scope(datasetName)
        client = RucioClient()
        try:
            # delete files
            client.detach_dids(scope=scope, name=dataset_name, dids=files)
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
    def registerContainer(self, cname, datasets=[], preset_scope=None):
        if cname.endswith("/"):
            cname = cname[:-1]
        # register container
        client = RucioClient()
        try:
            scope, dataset_name = self.extract_scope(cname)
            if preset_scope is not None:
                scope = preset_scope
            client.add_container(scope=scope, name=cname)
        except DataIdentifierAlreadyExists:
            pass
        # add files
        if len(datasets) > 0:
            try:
                dataset_names = []
                for ds in datasets:
                    ds_scope, ds_name = self.extract_scope(ds)
                    if ds_scope:
                        dataset_name = {"scope": ds_scope, "name": ds_name}
                    else:
                        dataset_name = {"scope": scope, "name": ds}
                    dataset_names.append(dataset_name)
                client.add_datasets_to_container(scope=scope, name=cname, dataset_names=dataset_names)
            except DuplicateContent:
                for ds in dataset_names:
                    try:
                        client.add_datasets_to_container(scope=scope, name=cname, dataset_names=[ds])
                    except DuplicateContent:
                        pass
        return True


# instantiate
rucioAPI = RucioAPI()
del RucioAPI
