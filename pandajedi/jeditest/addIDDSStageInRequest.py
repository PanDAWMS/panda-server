import sys

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandaserver.taskbuffer.DataCarousel import (
    DataCarouselInterface,
    DataCarouselRequestSpec,
    DataCarouselRequestStatus,
)

vo = "atlas"
task_id = int(sys.argv[1])
request_id = int(sys.argv[2])

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

print("query DB for data carousel request")
sql = (
    f"SELECT req.* "
    f"FROM ATLAS_PANDA.data_carousel_requests req, ATLAS_PANDA.data_carousel_relations rel "
    f"WHERE req.request_id=rel.request_id "
    f"AND rel.request_id=:request_id AND rel.task_id=:task_id "
)
var_map = {
    ":task_id": task_id,
    ":request_id": request_id,
}
res_list = tbIF.querySQL(sql, var_map)

if res_list:
    for res in res_list:
        print("got request; submit iDDS stage-in request")
        # make request spec
        dc_req_spec = DataCarouselRequestSpec()
        dc_req_spec.pack(res)
        # submit iDDS stage-in reqeust
        ret = data_carousel_interface._submit_idds_stagein_request(task_id, dc_req_spec)
        print(f"Done submit to iDDS; iDDS_requestID={ret}")
        break
else:
    print(f"Got no request: {res_list}")
    sys.exit(0)
