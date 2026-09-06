import sys

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction

# the map is keyed by (vo, group); group is absent for a VO-wide entry and the pair
# ("any", None) is the catchall. A configured-but-inactive plugin is stored as None.
InterfaceKey = tuple[str | None, str | None]


# interface to DDM
class DDMInterface:
    # constructor
    def __init__(self) -> None:
        self.interfaceMap: dict[InterfaceKey, Interaction.CommandSendInterface | None] = {}

    # setup interface
    def setupInterface(self) -> None:
        # parse config
        for configStr in jedi_config.ddm.modConfig.split(","):
            configStr = configStr.strip()
            items = configStr.split(":")
            # check format
            active = True
            # the optional group field is either the 5th item or absent. Declared
            # without a value, which binds nothing at runtime, so that the first
            # assignment below does not fix group to str alone.
            group: str | None
            try:
                vo = items[0]
                maxSize = int(items[1])
                moduleName = items[2]
                className = items[3]
                if len(items) >= 5:
                    group = items[4]
                    if not group:
                        group = None
                else:
                    group = None
                if len(items) >= 6 and items[5] == "off":
                    active = False
            except Exception:
                # TODO add config error message
                continue
            # add VO interface
            if active:
                voIF = Interaction.CommandSendInterface(vo, maxSize, moduleName, className)
                voIF.initialize()
            else:
                voIF = None
            key = self.get_dict_key(vo, group)
            self.interfaceMap[key] = voIF

    # get interface with VO
    # vo is Optional because a task spec can carry a NULL vo; such a call matches no VO
    # key and falls through to the "any" catchall below, which is the intended behaviour
    def getInterface(self, vo: str | None, group: str | None = None) -> Interaction.CommandSendInterface | None:
        # vo + group
        key = self.get_dict_key(vo, group)
        if key in self.interfaceMap:
            return self.interfaceMap[key]
        # only vo
        key = self.get_dict_key(vo, None)
        if key in self.interfaceMap:
            return self.interfaceMap[key]
        # catchall
        cacheAll = self.get_dict_key("any", None)
        if cacheAll in self.interfaceMap:
            return self.interfaceMap[cacheAll]
        # not found
        return None

    # get dict key
    def get_dict_key(self, vo: str | None, group: str | None) -> InterfaceKey:
        return vo, group


if __name__ == "__main__":

    def dummyClient(dif: DDMInterface) -> None:
        print("client test")
        interface = dif.getInterface("atlas")
        if interface is None:
            print("no interface configured for atlas")
            return
        interface.test()
        print("client done")

    dif = DDMInterface()
    dif.setupInterface()
    print("master test")
    atlasIF = dif.getInterface("atlas")
    if atlasIF is None:
        print("no interface configured for atlas")
        sys.exit(1)
    atlasIF.test()
    print("master done")
    import multiprocessing

    p = multiprocessing.Process(target=dummyClient, args=(dif,))
    p.start()
    p.join()
