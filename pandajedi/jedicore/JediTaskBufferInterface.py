from typing import Any

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction


# interface to JediTaskBuffer
class JediTaskBufferInterface:
    # constructor
    def __init__(self) -> None:
        self.interface: Interaction.CommandSendInterface | None = None

    # setup interface
    def setupInterface(self, max_size: int | None = None) -> None:
        vo = "any"
        maxSize = max_size if max_size is not None else jedi_config.db.nWorkers
        moduleName = "pandajedi.jedicore.JediTaskBuffer"
        className = "JediTaskBuffer"
        self.interface = Interaction.CommandSendInterface(vo, maxSize, moduleName, className)
        self.interface.initialize()

    # method emulation. Everything JEDI calls on this object is a JediTaskBuffer method
    # reached over a pipe, so a type checker can say nothing about any of them
    def __getattr__(self, attrName: str) -> Any:
        return getattr(self.interface, attrName)


if __name__ == "__main__":

    def dummyClient(dif: JediTaskBufferInterface, stime: int) -> None:
        print("client test")

        for i in range(3):
            # time.sleep(i*stime)
            try:
                print(dif.getCloudList())
            except Exception:
                print("exp")
        print("client done")

    dif = JediTaskBufferInterface()
    dif.setupInterface()
    print("master test")
    print(dif.getCloudList())
    print("master done")
    import multiprocessing

    pList = []
    for i in range(5):
        p = multiprocessing.Process(target=dummyClient, args=(dif, i))
        pList.append(p)
        p.start()
