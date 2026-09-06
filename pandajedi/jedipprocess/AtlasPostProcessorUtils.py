import copy
from typing import TYPE_CHECKING

from pandajedi.jedicore import JediException
from pandajedi.jedirefine import RefinerUtils

if TYPE_CHECKING:
    # Importing these for real makes this module read a configuration file at import time,
    # and it has no other reason to need one. Annotations are evaluated at runtime in this
    # tree, so the uses below are quoted.
    from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
    from pandajedi.jedicore.MsgWrapper import MsgWrapper
    from pandajedi.jediddm.AtlasDDMClient import AtlasDDMClient
    from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec

try:
    import idds.common.constants
    import idds.common.utils
    from idds.client.client import Client as iDDS_Client
except ImportError:
    pass


# send notification to external system for additional post-processing
# ddmIF is the backend client returned by DDMInterface.getInterface(), not the interface
# itself: extract_scope() below exists only on the ATLAS one, which is also the only caller
def send_notification(taskBufferIF: "JediTaskBufferInterface", ddmIF: "AtlasDDMClient", taskSpec: "JediTaskSpec", tmpLog: "MsgWrapper") -> None:
    # send notification to external system
    try:
        taskParam = taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
        taskParamMap = RefinerUtils.decodeJSON(taskParam)
    except Exception as e:
        errStr = f"task param conversion from json failed with {str(e)}"
        raise JediException.ExternalTempError(errStr)
    if "outputPostProcessing" in taskParamMap and "system" in taskParamMap["outputPostProcessing"]:
        if taskParamMap["outputPostProcessing"]["system"] == "idds":
            try:
                c = iDDS_Client(idds.common.utils.get_rest_host())
                if taskParamMap["outputPostProcessing"]["type"] == "active_learning":
                    for datasetSpec in taskSpec.datasetSpecList:
                        if datasetSpec.type != "output":
                            continue
                        data = copy.copy(taskParamMap["outputPostProcessing"]["data"])
                        tmp_scope, tmp_name = ddmIF.extract_scope(datasetSpec.datasetName)
                        data["workload_id"] = taskSpec.jediTaskID
                        req = {
                            "scope": tmp_scope,
                            "name": tmp_name,
                            "requester": "panda",
                            "request_type": idds.common.constants.RequestType.ActiveLearning,
                            "transform_tag": idds.common.constants.RequestType.ActiveLearning.value,
                            "status": idds.common.constants.RequestStatus.New,
                            "priority": 0,
                            "lifetime": 30,
                            "request_metadata": data,
                        }
                        tmpLog.debug(f"req {str(req)}")
                        ret = c.add_request(**req)
                        tmpLog.debug(f"got requestID={str(ret)}")
            except Exception as e:
                errStr = f"iDDS failed with {str(e)}"
                raise JediException.ExternalTempError(errStr)
