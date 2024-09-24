"""
Simple plugin of Adder for VOs with Rucio

"""

import datetime
import time
import traceback
import uuid

from pandaserver.dataservice import DataServiceUtils, ErrorCode
from pandaserver.dataservice.ddm import rucioAPI
from rucio.common.exception import (
    DataIdentifierNotFound,
    FileConsistencyMismatch,
    InvalidObject,
    InvalidPath,
    InvalidRSEExpression,
    RSEFileNameNotSupported,
    RSENotFound,
    RSEProtocolNotSupported,
    UnsupportedOperation,
)

from .adder_plugin_base import AdderPluginBase


class AdderSimplePlugin(AdderPluginBase):
    """
    Simple plugin of Adder for VOs with Rucio.
    """

    # constructor
    def __init__(self, job, **params)  -> None:
        """
        Initialize the AdderSimplePlugin.

        :param job: The job object.
        :param params: Additional parameters.
        """
        AdderPluginBase.__init__(self, job, params)

    # main
    def execute(self) -> None:
        """
        Execute the simple adder plugin.

        :return: None
        """
        try:
            # loop over all files
            file_map = {}
            for file_spec in self.job.Files:
                # ignore inputs
                if file_spec.type not in ["output", "log"]:
                    continue
                # ignore local
                if file_spec.destinationSE == "local":
                    continue
                # collect file attributes
                try:
                    file_size = int(file_spec.fsize)
                except Exception:
                    file_size = None
                # set GUID if empty
                if not file_spec.GUID:
                    file_spec.GUID = str(uuid.uuid4())
                file_attrs = {
                    "guid": file_spec.GUID,
                    "lfn": file_spec.lfn,
                    "size": file_size,
                    "checksum": file_spec.checksum,
                    "ds": file_spec.destinationDBlock,
                }
                if self.extra_info:
                    if "surl" in self.extra_info and file_spec.lfn in self.extra_info["surl"]:
                        file_attrs["surl"] = self.extra_info["surl"][file_spec.lfn]
                    if "nevents" in self.extra_info and file_spec.lfn in self.extra_info["nevents"]:
                        file_attrs["events"] = self.extra_info["nevents"][file_spec.lfn]
                file_map.setdefault(file_spec.destinationDBlock, [])
                file_map[file_spec.destinationDBlock].append(file_attrs)
            # register files
            if file_map:
                destination_rse = self.siteMapper.getSite(self.job.computingSite).ddm_output["default"]
                destination_id_map = {destination_rse: file_map}
                max_attempt = 3
                for attempt_number in range(max_attempt):
                    is_fatal = False
                    is_failed = False
                    registration_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                    try:
                        self.logger.debug(f"registerFilesInDatasets {str(destination_id_map)}")
                        out = rucioAPI.register_files_in_dataset(destination_id_map, {})
                    except (
                        DataIdentifierNotFound,
                        FileConsistencyMismatch,
                        UnsupportedOperation,
                        InvalidPath,
                        InvalidObject,
                        RSENotFound,
                        RSEProtocolNotSupported,
                        InvalidRSEExpression,
                        RSEFileNameNotSupported,
                        KeyError,
                    ) as e:
                        # fatal errors
                        out = f"failed with {str(e)}\n {traceback.format_exc()}"
                        is_fatal = True
                        is_failed = True
                    except Exception as e:
                        # unknown errors
                        is_failed = True
                        out = f"failed with unknown error: {str(e)}\n {traceback.format_exc()}"
                        if (
                            "value too large for column" in out
                            or "unique constraint (ATLAS_RUCIO.DIDS_GUID_IDX) violate" in out
                            or "unique constraint (ATLAS_RUCIO.DIDS_PK) violated" in out
                            or "unique constraint (ATLAS_RUCIO.ARCH_CONTENTS_PK) violated" in out
                        ):
                            is_fatal = True
                        else:
                            is_fatal = False
                    registration_time = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - registration_start
                    self.logger.debug("took %s.%03d sec" % (registration_time.seconds, registration_time.microseconds / 1000))

                    # failed
                    if is_failed or is_fatal:
                        self.logger.error(f"{out}")
                        if (attempt_number + 1) == max_attempt or is_fatal:
                            self.job.ddmErrorCode = ErrorCode.EC_Adder
                            # extract important error string
                            extracted_error = DataServiceUtils.extractImportantError(out)
                            err_msg = "Could not add files to DDM: "
                            if extracted_error == "":
                                self.job.ddmErrorDiag = err_msg + out.split("\n")[-1]
                            else:
                                self.job.ddmErrorDiag = err_msg + extracted_error
                            if is_fatal:
                                self.err_str.set_fatal()
                            else:
                                self.result.set_temporary()
                            return 1
                        self.logger.error(f"Try:{attempt_number}")
                        # sleep
                        time.sleep(10)
                    else:
                        self.logger.debug(f"{str(out)}")
                        break
            # done
            self.result.set_succeeded()
            self.logger.debug("end plugin")
        except Exception as e:
            err_str = f"failed to execute with {str(e)}\n"
            err_str += traceback.format_exc()
            self.logger.error(err_str)
            self.result.set_temporary()
        # return
        return
