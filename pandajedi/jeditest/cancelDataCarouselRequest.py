import sys

from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedirefine import RefinerUtils
from pandaserver.taskbuffer.DataCarousel import DataCarouselInterface

vo = "atlas"
request_id = int(sys.argv[1])

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

print(f"cancel request_id={request_id}")
ret = data_carousel_interface.cancel_request(request_id)

if not ret:
    # failed to cancel
    print(f"failed; got {ret}")
else:
    # cancelled
    print(f"cancelled, got {ret}")
