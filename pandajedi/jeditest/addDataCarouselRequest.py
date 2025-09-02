import sys

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedirefine import RefinerUtils
from pandaserver.taskbuffer.DataCarousel import DataCarouselInterface

vo = "atlas"
jediTaskID = int(sys.argv[1])

print("set tbIF")
tbIF = JediTaskBufferInterface()
tbIF.setupInterface(max_size=1)

print("set ddmIF")
ddmIF = DDMInterface()
ddmIF.setupInterface()

print("set DCIF")
data_carousel_interface = DataCarouselInterface(tbIF, ddmIF.getInterface(vo))
if data_carousel_interface is None:
    # data carousel interface is undefined
    errStr = f"data carousel interface is undefined for vo={vo}"
    print(errStr)
    sys.exit(1)

print(f"get task params of {jediTaskID}")
taskParam = tbIF.getTaskParamsWithID_JEDI(jediTaskID)
taskParamMap = RefinerUtils.decodeJSON(taskParam)

print(f"get_input_datasets_to_prestage")
prestaging_list, ret_map = data_carousel_interface.get_input_datasets_to_prestage(jediTaskID, taskParamMap)

if not prestaging_list:
    # no dataset needs pre-staging; unset inputPreStaging
    print("no need to prestage")
    # resume the task from staging
    print(f"resume task {jediTaskID}")
    tbIF.sendCommandTaskPanda(jediTaskID, "Test addDataCarouselRequest. No need to prestage. Resumed from staging", True, "resume")
else:
    # submit data carousel requests for dataset to pre-stage
    print("to prestage, submitting data carousel requests")
    tmp_ret = data_carousel_interface.submit_data_carousel_requests(jediTaskID, prestaging_list)
    if tmp_ret:
        print(f"submitted data carousel requests for {jediTaskID}: {tmp_ret}")
    else:
        print(f"failed to submit data carousel requests")
