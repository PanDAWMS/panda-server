from typing import TYPE_CHECKING, Any

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction

if TYPE_CHECKING:
    # for the annotation only, so that importing this module, as the JEDI master does at
    # startup, does not also load SiteMapper and the pandaserver config it pulls in
    from pandaserver.brokerage.SiteMapper import SiteMapper


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

    # reached over the pipe like the methods below, and named here only so that what it
    # returns is declared. The pipe hands back whatever JediTaskBuffer.get_site_mapper()
    # built in the child process
    def get_site_mapper(self) -> "SiteMapper":
        if self.interface is None:
            raise Interaction.JEDIFatalError("setupInterface() has not been called")
        site_mapper: "SiteMapper" = self.interface.get_site_mapper()
        return site_mapper

    # method emulation. Everything else JEDI calls on this object is a JediTaskBuffer method
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
