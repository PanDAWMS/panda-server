import json
import sys

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedirefine import RefinerUtils
from pandaserver.taskbuffer.DataCarousel import DataCarouselInterface

vo = "atlas"
request_id = int(sys.argv[1])
json_to_update = str(sys.argv[2])

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

with data_carousel_interface.global_dc_lock(timeout_sec=60, lock_expiration_sec=300) as full_pid:
    # timeout
    if full_pid is None:
        print(f"timed out without getting lock")
        sys.exit(1)

    print(f"get spec request_id={request_id}")
    dc_req_spec = None
    if request_id is not None:
        # specified by request_id
        dc_req_spec = data_carousel_interface.get_request_by_id(request_id)

    # update parameters
    print(f"update parameters of request_id={request_id}")
    if dc_req_spec and (dict_to_update := json.loads(json_to_update)):
        dc_req_spec.update_parameters(dict_to_update)
        tbIF.update_data_carousel_request_JEDI(dc_req_spec)
        print(f"updated request_id={request_id} with parameters: {dict_to_update}")
    elif dc_req_spec is None:
        print(f"request_id={request_id} not found")
    else:
        print(f"no parameters to update for request_id={request_id}")
