import os
import subprocess
import tempfile


class eventLookupClientEI:
    def __init__(self):
        pass

    def doLookup(self, runEvtList, stream=None, tokens=None, amitag=None, user=None, ei_api=None):
        command = os.path.join(
            os.getenv(
                "EIDIR",
                "/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/EIClient/current",
            ),
            "bin",
            "event-lookup",
        )
        tempEvtFile = tempfile.NamedTemporaryFile(mode="w+t")
        command += f" -F {tempEvtFile.name} "
        for runEvt in runEvtList:
            tmpStr = f"{int(runEvt[0]):08d} {int(runEvt[1]):09d}\n"
            tempEvtFile.write(tmpStr)
        tempEvtFile.flush()
        if stream not in [None, ""]:
            command += f"-s {stream} "
        if amitag not in [None, ""]:
            command += f"-a {amitag} "
        command += "-c plain "
        p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True,
        )
        tmpOut, tmpErr = p.communicate()
        tempEvtFile.close()
        guids = {}
        if tokens == "":
            tokens = None
        try:
            for tmpLine in tmpOut.split("\n"):
                tmpItems = tmpLine.split()
                runEvent = (int(tmpItems[0]), int(tmpItems[1]))
                guids.setdefault(runEvent, set())
                # check type
                tmpToken = "Stream" + tmpItems[3]
                tmpGUID = tmpItems[2]
                if not tokens or tokens == tmpToken:
                    guids[runEvent].add(tmpGUID)
            if not guids:
                # add dummy
                guids[None] = None
        except Exception:
            pass
        return guids, command, tmpOut, tmpErr
