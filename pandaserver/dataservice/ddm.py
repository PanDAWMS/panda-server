"""
Module to provide primitive methods for DDM

"""

import hashlib
import re
import sys
import traceback
from typing import Dict, List

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
                file_scope, lfn = lfn.split(":")[0], lfn.split(":")[1]
            else:
                file_scope = scope
            file = {"scope": file_scope, "name": lfn, "bytes": size, "meta": {"guid": guid}}
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
                    for tmp_file in tmp_files:
                        try:
                            client.add_files_to_dataset(scope=scope, name=dataset_name, files=[tmp_file], rse=None)
                        except FileAlreadyExists:
                            pass
                i_files += n_files
        # Format is a 36-character string divided into five groups separated by hyphens. The groups have 8, 4, 4, 4, and 12 characters.
        # After the formatting, the vuid string would look something like this: 12345678-1234-1234-1234-123456789012
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
        did = {"scope": scope, "name": dataset_name}
        dids = [did]
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
    def get_user(self, client, distinguished_name: str) -> str:
        """
        This method retrieves the account name associated with a given distinguished name (dn) from the Rucio client.
        If no account is found, it returns the default account of the Rucio client.

        Parameters:
        client (RucioClient): The Rucio client instance to interact with the Rucio server.
        dn (str): The distinguished name (dn) for which the associated account is to be retrieved.

        Returns:
        str: The account name associated with the given dn. If no account is found, it returns the default account of the Rucio client.
        """
        tmp_list = list(client.list_accounts("user", distinguished_name))
        if tmp_list:
            owner = tmp_list[0]["account"]
            return owner
        return client.account

    # register dataset subscription
    def register_dataset_subscription(
        self, dataset_name: str, rses: list, lifetime: int = None, owner: str = None, activity: str = None, distinguished_name: str = None, comment: str = None
    ) -> bool:
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
        distinguished_name (str, optional): The distinguished name of the user. Defaults to None.
        comment (str, optional): A comment to be associated with the replication rule. Defaults to None.

        Returns:
        bool: True if the operation is successful, False otherwise.
        """
        lifetime = lifetime * 24 * 60 * 60 if lifetime else lifetime
        scope, dataset_name = self.extract_scope(dataset_name)
        did = {"scope": scope, "name": dataset_name}
        dids = [did]
        # make location
        rses.sort()
        location = "|".join(rses)
        # check if a replication rule already exists
        client = RucioClient()
        # owner
        if owner is None:
            if distinguished_name is not None:
                owner = self.get_user(client, distinguished_name)
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
    def convert_file_attributes(self, tmp_file: dict, scope: str) -> dict:
        """
        Convert file attribute to a dictionary

        Parameters:
        tmp_file (dict): File attribute
        scope (str): Scope of the file

        Returns:
        dict: A dictionary containing file attributes
        """
        lfn = tmp_file.get("name", tmp_file.get("lfn"))
        file_scope, lfn = lfn.split(":") if ":" in lfn else (scope, lfn)
        # set metadata
        meta_keys = ["guid", "events", "lumiblocknr", "panda_id", "campaign", "task_id"]
        meta = {key: tmp_file[key] for key in meta_keys if key in tmp_file}
        file_size = tmp_file.get("bytes", tmp_file.get("size"))
        file = {"scope": file_scope, "name": lfn, "bytes": file_size, "meta": meta}
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
    def register_files_in_dataset(self, id_map: dict, files_without_rses: list = None) -> bool:
        """
        Register files in a dataset

        Parameters:
        id_map (dict): A dictionary containing dataset information. Maps RSEs to datasets and files.
        files_without_rses (list, optional): List of files without RSEs. Defaults to None.

        Returns:
        bool: True if the operation is successful, False otherwise
        """
        # loop over all rse
        attachment_list = []
        for rse in id_map:
            tmp_map = id_map[rse]
            # loop over all datasets
            for dataset_name in tmp_map:
                file_list = tmp_map[dataset_name]
                # extract scope from dataset
                scope, given_dataset_name = self.extract_scope(dataset_name)
                files_with_rse = []
                files_without_rse = []
                for tmp_file in file_list:
                    # convert file attribute
                    file = self.convert_file_attributes(tmp_file, scope)
                    # append files
                    if rse is not None and (files_without_rses is None or file["name"] not in files_without_rses):
                        files_with_rse.append(file)
                    else:
                        if "pfn" in file:
                            del file["pfn"]
                        files_without_rse.append(file)
                # add attachment
                if len(files_with_rse) > 0:
                    n_files = 100
                    i_files = 0
                    while i_files < len(files_with_rse):
                        attachment = {
                            "scope": scope,
                            "name": given_dataset_name,
                            "dids": files_with_rse[i_files : i_files + n_files],
                            "rse": rse,
                        }
                        attachment_list.append(attachment)
                        i_files += n_files
                if len(files_without_rse) > 0:
                    n_files = 100
                    i_files = 0
                    while i_files < len(files_without_rse):
                        attachment = {
                            "scope": scope,
                            "name": given_dataset_name,
                            "dids": files_without_rse[i_files : i_files + n_files],
                        }
                        attachment_list.append(attachment)
                        i_files += n_files
        # add files
        client = RucioClient()
        client.add_files_to_datasets(attachment_list, ignore_duplicate=True)
        return True

    # register zip files
    def register_zip_files(self, zip_map: dict) -> None:
        """
        Register zip files in Rucio.

        This method takes a map of zip file names to their attributes and registers the zip files in Rucio.
        It first converts the zip file attributes to a dictionary format that Rucio can understand.
        Then, it loops over all the files contained in the zip file, converts their attributes to the Rucio dictionary format, and appends them to a list.
        Finally, it registers the zip file and its contained files in Rucio.

        Parameters:
        zip_map (dict): A dictionary mapping zip file names to their attributes. The structure is {zipFileName: zipFileAttr, ...}.

        Returns:
        None
        """
        # no zip files
        if len(zip_map) == 0:
            return
        client = RucioClient()
        # loop over all zip files
        for zip_file_name in zip_map:
            zip_file_attr = zip_map[zip_file_name]
            # convert file attribute
            zip_file = self.convert_file_attributes(zip_file_attr, zip_file_attr["scope"])
            # loop over all contents
            files = []
            for con_file_attr in zip_file_attr["files"]:
                # get scope
                scope, _ = self.extract_scope(con_file_attr["ds"])
                # convert file attribute
                con_file = self.convert_file_attributes(con_file_attr, scope)
                con_file["type"] = "FILE"
                if "pfn" in con_file:
                    del con_file["pfn"]
                # append files
                files.append(con_file)
            # register zip file
            for rse in zip_file_attr["rse"]:
                client.add_replicas(rse=rse, files=[zip_file])
            # add files
            n_files = 100
            i_files = 0
            while i_files < len(files):
                client.add_files_to_archive(
                    scope=zip_file["scope"],
                    name=zip_file["name"],
                    files=files[i_files : i_files + n_files],
                )
                i_files += n_files

    # list datasets
    def list_datasets(self, dataset_name: str, old: bool = False):
        """
        List datasets in Rucio.

        This method lists datasets in Rucio by extracting the scope from the dataset name and getting the DIDs (Data Identifiers).
        It generates a unique identifier (vuid) for each DID and stores it in a dictionary along with the dataset name.
        If an exception occurs during the process, it returns None and the exception message.

        Parameters:
        dataset_name (str): The name of the dataset.
        old (bool, optional): A flag to indicate if the dataset is old. Defaults to False.

        Returns:
        Tuple[Union[dict, None], str]: A tuple containing a dictionary of datasets and their unique identifiers, and a string message.
        If an exception occurs, the dictionary is None and the string contains the exception message.
        """
        result = {}
        # extract scope from dataset
        scope, given_dataset_name = self.extract_scope(dataset_name)
        if given_dataset_name.endswith("/"):
            given_dataset_name = given_dataset_name[:-1]
            collection = "container"
        else:
            collection = "dataset"
        filters = {"name": given_dataset_name}
        try:
            # get dids
            client = RucioClient()
            for name in client.list_dids(scope, filters, collection):
                vuid = hashlib.md5((scope + ":" + name).encode()).hexdigest()
                # Format is a 36-character string divided into five groups separated by hyphens. The groups have 8, 4, 4, 4, and 12 characters.
                # After the formatting, the vuid string would look something like this: 12345678-1234-1234-1234-123456789012
                vuid = f"{vuid[0:8]}-{vuid[8:12]}-{vuid[12:16]}-{vuid[16:20]}-{vuid[20:32]}"
                duid = vuid
                # add /
                if dataset_name.endswith("/") and not name.endswith("/"):
                    name += "/"
                if old or ":" not in dataset_name:
                    key_name = name
                else:
                    key_name = str(f"{scope}:{name}")
                if key_name not in result:
                    result[key_name] = {"duid": duid, "vuids": [vuid]}
            return result, ""
        except Exception as error:
            return None, f"{str(error)} {traceback.format_exc()}"

    # list datasets in container
    def list_datasets_in_container(self, container_name: str):
        """
        List datasets in a Rucio container.

        This method lists datasets in a Rucio container by extracting the scope from the container name and getting the DIDs (Data Identifiers).
        It generates a unique identifier (vuid) for each DID and stores it in a list.
        If an exception occurs during the process, it returns None and the exception message.

        Parameters:
        container_name (str): The name of the container.

        Returns:
        Tuple[Union[None, List[str]], str]: A tuple containing a list of datasets and a string message.
        If an exception occurs, the list is None and the string contains the exception message.
        """
        result = []
        # extract scope from dataset
        scope, container_name = self.extract_scope(container_name)
        if container_name.endswith("/"):
            container_name = container_name[:-1]
        try:
            # get dids
            client = RucioClient()
            for content in client.list_content(scope, container_name):
                if content["type"] == "DATASET":
                    result.append(str(f"{content['scope']}:{content['name']}"))
            return result, ""
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            return None, f"{err_type} {err_value}"

    # list dataset replicas
    def list_dataset_replicas(self, dataset_name: str):
        """
        List dataset replicas in Rucio.

        This method lists dataset replicas in Rucio by extracting the scope from the dataset name and getting the replicas.
        It stores the replicas in a dictionary and returns it along with a status code.
        If an exception occurs during the process, it returns the error type and value.

        Parameters:
        dataset_name (str): The name of the dataset.

        Returns:
        Tuple[int, Union[str, dict]]: A tuple containing a status code and a dictionary of dataset replicas or an error message.
        If an exception occurs, the dictionary is None and the string contains the error message.
        """
        return_map = {}
        # extract scope from dataset
        scope, dataset_name = self.extract_scope(dataset_name)
        try:
            # get replicas
            client = RucioClient()
            replica_iterator = client.list_dataset_replicas(scope, dataset_name)
            for item in replica_iterator:
                rse = item["rse"]
                return_map[rse] = [
                    {
                        "total": item["length"],
                        "found": item["available_length"],
                        "immutable": 1,
                    }
                ]
            return 0, return_map
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            return 1, f"{err_type} {err_value}"

    # get metadata
    def get_metadata(self, dataset_name: str, scope: str = None):
        """
        Get metadata of a dataset in Rucio.

        This method retrieves the metadata of a dataset in Rucio by extracting the scope from the dataset name and getting the metadata.
        If an exception occurs during the process, it returns False and the exception message.

        Parameters:
        dataset_name (str): The name of the dataset.
        scope (str, optional): The scope of the dataset. Defaults to None.

        Returns:
        Tuple[bool, Union[None, dict]]: A tuple containing a boolean indicating the success of the operation and a dictionary of metadata or an error message.
        If an exception occurs, the boolean is False and the string contains the error message.
        """
        # register dataset
        client = RucioClient()
        try:
            scope, dataset_name = self.extract_scope(dataset_name)
            return True, client.get_metadata(scope, dataset_name)
        except DataIdentifierNotFound:
            return True, None
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            return False, f"{err_type} {err_value}"

    # delete dataset
    def erase_dataset(self, dataset_name: str, scope: str = None, grace_period: int = None):
        """
        Delete a dataset in Rucio.

        This method deletes a dataset in Rucio by extracting the scope from the dataset name and calling the erase method.
        If an exception occurs during the process, it returns False and the exception message.

        Parameters:
        dataset_name (str): The name of the dataset.
        scope (str, optional): The scope of the dataset. Defaults to None.
        grace_period (int, optional): The grace period before deletion. Defaults to None.

        Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating the success of the operation and a string message.
        If an exception occurs, the boolean is False and the string contains the error message.
        """
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
        except Exception as error:
            return False, str(error)
        return True, ""

    # close dataset
    def close_dataset(self, dataset_name: str) -> bool:
        """
        Close a dataset in Rucio.

        This method closes a dataset in Rucio by extracting the scope from the dataset name and setting the status of the dataset to closed.
        If an exception occurs during the process, it ignores the exception and returns True.

        Parameters:
        dataset_name (str): The name of the dataset.

        Returns:
        bool: True if the operation is successful, False otherwise.
        """
        # register dataset
        client = RucioClient()
        try:
            scope, dataset_name = self.extract_scope(dataset_name)
            client.set_status(scope, dataset_name, open=False)
        except (UnsupportedOperation, DataIdentifierNotFound):
            pass
        return True

    # list file replicas
    def list_file_replicas(self, scopes: List[str], lfns: List[str], rses: List[str] = None):
        """
        List file replicas in Rucio.

        This method lists file replicas in Rucio by creating a list of dictionaries containing the scope and name of each file.
        It then retrieves the replicas for these files from Rucio and stores them in a dictionary.
        If an exception occurs during the process, it returns False and the exception message.

        Parameters:
        scopes (List[str]): A list of scopes for the files.
        lfns (List[str]): A list of Logical File Names (LFNs) for the files.
        rses (List[str], optional): A list of Rucio Storage Elements (RSEs) where the files should be replicated. Defaults to None.

        Returns:
        Tuple[bool, Union[str, Dict[str, List[str]]]]: A tuple containing a boolean indicating the success of the operation and a dictionary of file replicas or an error message.
        If an exception occurs, the boolean is False and the string contains the error message.
        """
        try:
            client = RucioClient()
            dids = []
            i_guid = 0
            batch_size = 1000
            ret_val = {}
            for scope, lfn in zip(scopes, lfns):
                i_guid += 1
                dids.append({"scope": scope, "name": lfn})
                if len(dids) % batch_size == 0 or i_guid == len(lfns):
                    for tmp_dict in client.list_replicas(dids):
                        tmp_lfn = str(tmp_dict["name"])
                        tmp_rses = list(tmp_dict["rses"])
                        # RSE selection
                        if rses is not None:
                            tmp_rses = [tmp_rse for tmp_rse in tmp_rses if tmp_rse in rses]
                        if len(tmp_rses) > 0:
                            ret_val[tmp_lfn] = tmp_rses
                    dids = []
            return True, ret_val
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            return False, f"{err_type} {err_value}"

    # get zip files
    def get_zip_files(self, dids: List[str], rses: List[str]):
        """
        Get zip files from Rucio.

        This method retrieves zip files from Rucio by creating a list of dictionaries containing the scope and name of each file.
        It then retrieves the replicas for these files from Rucio and stores them in a dictionary.
        If an exception occurs during the process, it returns False and the exception message.

        Parameters:
        dids (List[str]): A list of Data Identifiers (DIDs) for which to retrieve the associated zip files.
        rses (List[str]): A list of Rucio Storage Elements (RSEs) where the files should be replicated.

        Returns:
        Tuple[bool, Union[str, Dict[str, Dict[str, Any]]]]: A tuple containing a boolean indicating the success of the operation and a dictionary of zip files or an error message.
        If an exception occurs, the boolean is False and the string contains the error message.
        """
        try:
            client = RucioClient()
            data = []
            i_guid = 0
            batch_size = 1000
            ret_val = {}
            for did in dids:
                i_guid += 1
                scope, lfn = did.split(":")
                data.append({"scope": scope, "name": lfn})
                if len(data) % batch_size == 0 or i_guid == len(dids):
                    for tmp_dict in client.list_replicas(data):
                        tmp_scope = str(tmp_dict["scope"])
                        tmp_lfn = str(tmp_dict["name"])
                        tmp_did = f"{tmp_scope}:{tmp_lfn}"
                        # RSE selection
                        for pfn in tmp_dict["pfns"]:
                            pfn_data = tmp_dict["pfns"][pfn]
                            if (rses is None or pfn_data["rse"] in rses) and pfn_data["domain"] == "zip":
                                zip_file_name = pfn.split("/")[-1]
                                zip_file_name = re.sub("\?.+$", "", zip_file_name)
                                ret_val[tmp_did] = client.get_metadata(tmp_scope, zip_file_name)
                                break
                    data = []
            return True, ret_val
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            return False, f"{err_type} {err_value}"

    # list files in dataset
    def list_files_in_dataset(self, dataset_name: str, long: bool = False, file_list: List[str] = None):
        """
        List files in a Rucio dataset.

        This method lists files in a Rucio dataset by extracting the scope from the dataset name and getting the files.
        It stores the files in a dictionary and returns it along with a status code.
        If an exception occurs during the process, it returns the error type and value.

        Parameters:
        dataset_name (str): The name of the dataset.
        long (bool, optional): A flag to indicate if the file list should be long. Defaults to False.
        file_list (List[str], optional): A list of files to be listed. Defaults to None.

        Returns:
        Tuple[Dict[str, Dict[str, Any]], Optional[str]]: A tuple containing a dictionary of files and a string message.
        If an exception occurs, the dictionary is None and the string contains the error message.
        """
        # extract scope from dataset
        scope, dataset_name = self.extract_scope(dataset_name)
        if dataset_name.endswith("/"):
            dataset_name = dataset_name[:-1]
        client = RucioClient()
        return_dict = {}
        for file_info in client.list_files(scope, dataset_name, long=long):
            tmp_lfn = str(file_info["name"])
            if file_list is not None:
                gen_lfn = re.sub("\.\d+$", "", tmp_lfn)
                if tmp_lfn not in file_list and gen_lfn not in file_list:
                    continue
            rucio_attrs = {}
            rucio_attrs["chksum"] = "ad:" + str(file_info["adler32"])
            rucio_attrs["md5sum"] = rucio_attrs["chksum"]
            rucio_attrs["checksum"] = rucio_attrs["chksum"]
            rucio_attrs["fsize"] = file_info["bytes"]
            rucio_attrs["filesize"] = rucio_attrs["fsize"]
            rucio_attrs["scope"] = str(file_info["scope"])
            rucio_attrs["events"] = str(file_info["events"])
            if long:
                rucio_attrs["lumiblocknr"] = str(file_info["lumiblocknr"])
            guid = str(f"{file_info['guid'][0:8]}-{file_info['guid'][8:12]}-{file_info['guid'][12:16]}-{file_info['guid'][16:20]}-{file_info['guid'][20:32]}")
            rucio_attrs["guid"] = guid
            return_dict[tmp_lfn] = rucio_attrs
        return (return_dict, None)

    # get # of files in dataset
    def get_number_of_files(self, dataset_name: str, preset_scope: str = None):
        """
        Get the number of files in a Rucio dataset.

        This method retrieves the number of files in a Rucio dataset by extracting the scope from the dataset name and getting the files.
        If an exception occurs during the process, it returns False and the exception message.

        Parameters:
        dataset_name (str): The name of the dataset.
        preset_scope (str, optional): The scope of the dataset. Defaults to None.

        Returns:
        Tuple[bool, Union[int, str]]: A tuple containing a boolean indicating the success of the operation and the number of files or an error message.
        If an exception occurs, the boolean is False and the string contains the error message.
        """
        # extract scope from dataset
        scope, dataset_name = self.extract_scope(dataset_name)
        if preset_scope is not None:
            scope = preset_scope
        client = RucioClient()
        n_files = 0
        try:
            for _ in client.list_files(scope, dataset_name):
                n_files += 1
            return True, n_files
        except DataIdentifierNotFound:
            return None, "dataset not found"
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            err_msg = f"{err_type.__name__} {err_value}"
            return False, err_msg

    # list datasets with GUIDs
    def list_datasets_by_guids(self, guids: List[str]) -> Dict[str, List[str]]:
        """
        List datasets in Rucio by GUIDs.

        This method lists datasets in Rucio by their GUIDs. It retrieves the datasets associated with each GUID
        and stores them in a dictionary where the keys are the GUIDs and the values are lists of datasets.

        Parameters:
        guids (List[str]): A list of GUIDs for which to retrieve the associated datasets.

        Returns:
        Dict[str, List[str]]: A dictionary mapping each GUID to a list of its associated datasets.
        """
        client = RucioClient()
        result = {}
        for guid in guids:
            datasets = [str(f"{i['scope']}:{i['name']}") for i in client.get_dataset_by_guid(guid)]
            result[guid] = datasets
        return result

    # register container
    def register_container(self, container_name: str, datasets: List[str] = None, preset_scope: str = None) -> bool:
        """
        Register a container in Rucio.

        This method registers a container in Rucio by extracting the scope from the container name and adding the container.
        If the container already exists, it ignores the exception and continues.
        If a list of datasets is provided, it adds these datasets to the container.

        Parameters:
        container_name (str): The name of the container.
        datasets (List[str], optional): A list of datasets to be added to the container. Defaults to None.
        preset_scope (str, optional): The scope of the container. Defaults to None.

        Returns:
        bool: True if the operation is successful, False otherwise.
        """
        if container_name.endswith("/"):
            container_name = container_name[:-1]
        # register container
        client = RucioClient()
        try:
            scope, dataset_name = self.extract_scope(container_name)
            if preset_scope is not None:
                scope = preset_scope
            client.add_container(scope=scope, name=container_name)
        except DataIdentifierAlreadyExists:
            pass
        # add files
        if datasets is not None and len(datasets) > 0:
            try:
                dataset_names = []
                for dataset in datasets:
                    dataset_scope, dataset_name = self.extract_scope(dataset)
                    if dataset_scope:
                        dataset_name = {"scope": dataset_scope, "name": dataset_name}
                    else:
                        dataset_name = {"scope": scope, "name": dataset}
                    dataset_names.append(dataset_name)
                client.add_datasets_to_container(scope=scope, name=container_name, dsns=dataset_names)
            except DuplicateContent:
                for dataset in dataset_names:
                    try:
                        client.add_datasets_to_container(scope=scope, name=container_name, dsns=[dataset])
                    except DuplicateContent:
                        pass
        return True

    # finger
    def finger(self, distinguished_name: str):
        """
        Retrieve user information from Rucio based on the distinguished name (dn).

        This method retrieves user information from Rucio by using the distinguished name (dn) to identify the user.
        It first checks if the user is identified by an X509 certificate or an OIDC token.
        It then iterates over the list of accounts in Rucio, looking for a match with the user's distinguished name.
        If a match is found, it retrieves the user's nickname and email and stores them in a dictionary.
        If no match is found, it attempts to retrieve the account information directly using the distinguished name.
        If an exception occurs during the process, it returns the error message.

        Parameters:
        distinguished_name (str): The distinguished name of the user.

        Returns:
        Tuple[bool, Union[dict, str]]: A tuple containing a boolean indicating the success of the operation and a dictionary of user information or an error message.
        If an exception occurs, the boolean is False and the string contains the error message.
        """
        try:
            # get rucio API
            client = RucioClient()
            user_info = None
            return_value = False
            x509_user_name = CoreUtils.get_bare_dn(distinguished_name)
            oidc_user_name = CoreUtils.get_id_from_dn(distinguished_name)
            if oidc_user_name == x509_user_name:
                oidc_user_name = None
            else:
                x509_user_name = None
            for account_type in ["USER", "GROUP"]:
                if x509_user_name is not None:
                    user_names = [x509_user_name]
                    # replace / with , and reverse substrings to be converted to RFC format
                    tmp_list = user_names[-1].split("/")
                    if "" in tmp_list:
                        tmp_list.remove("")
                    user_names.append(",".join(tmp_list[::-1]))
                    # remove /CN=\d
                    user_names.append(CoreUtils.get_bare_dn(distinguished_name, keep_digits=False))
                    # replace / with , and reverse substrings to be converted to RFC format
                    tmp_list = user_names[-1].split("/")
                    if "" in tmp_list:
                        tmp_list.remove("")
                    user_names.append(",".join(tmp_list[::-1]))
                    for user_name in user_names:
                        for i in client.list_accounts(account_type=account_type, identity=user_name):
                            user_info = {"nickname": i["account"], "email": i["email"]}
                            break
                        if user_info is not None:
                            break
                else:
                    user_name = oidc_user_name
                try:
                    if user_info is None:
                        account = client.get_account(user_name)
                        user_info = {"nickname": account["account"], "email": account["email"]}
                except Exception:
                    pass
                if user_info is not None:
                    return_value = True
                    break
        except Exception as error:
            error_message = f"{str(error)}"
            user_info = error_message
        return return_value, user_info


# instantiate
rucioAPI = RucioAPI()
del RucioAPI
