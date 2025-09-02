from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction


# interface to DDM
class DDMInterface:
    # constructor
    def __init__(self):
        self.interfaceMap = {}

    # setup interface
    def setupInterface(self):
        # parse config
        for configStr in jedi_config.ddm.modConfig.split(","):
            configStr = configStr.strip()
            items = configStr.split(":")
            # check format
            active = True
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
    def getInterface(self, vo, group=None):
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
    def get_dict_key(self, vo, group):
        return vo, group


if __name__ == "__main__":

    def dummyClient(dif):
        print("client test")
        dif.getInterface("atlas").test()
        print("client done")

    dif = DDMInterface()
    dif.setupInterface()
    print("master test")
    atlasIF = dif.getInterface("atlas")
    atlasIF.test()
    print("master done")
    import multiprocessing

    p = multiprocessing.Process(target=dummyClient, args=(dif,))
    p.start()
    p.join()
