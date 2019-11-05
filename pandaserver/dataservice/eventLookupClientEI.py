import os
import tempfile
import subprocess

try:
    long
except NameError:
    long = int


class eventLookupClientEI:

    def __init__(self):
        pass


    def doLookup(self,runEvtList,stream=None,tokens=None,amitag=None,user=None,ei_api=None):
        command = 'java -jar ' + \
            os.getenv('EIDIR', '/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/EIClient/1.24.0') + \
            '/lib/EIHadoopEL.exe.jar '
        tempEvtFile = tempfile.NamedTemporaryFile()
        command += "-f {0} ".format(tempEvtFile.name)
        for runEvt in runEvtList:
            tmpStr = '{0:08d} {1:09d}\n'.format(long(runEvt[0]),long(runEvt[1]))
            tempEvtFile.write(tmpStr)
        tempEvtFile.flush()
        if not stream in [None,'']:
            command += "-s {0} ".format(stream)
        if not amitag in [None,'']:
            command += "-p {0} ".format(amitag)
        if user is not None:
            command += '-info "{0}" '.format(user)
        if ei_api:
            command += '-api {0} '.format(ei_api)
        command += r"""-details richtype """
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        tmpOut,tmpErr = p.communicate()
        tempEvtFile.close()
        guids = {}
        if tokens == '':
            tokens = None
        try:
            setRunEvent = False
            runEvent = None
            for tmpLine in tmpOut.split('\n'):
                if tmpLine == '>>>':
                    setRunEvent = True
                    continue
                if setRunEvent:
                    tmpItems = tmpLine.split('-')
                    runEvent = (long(tmpItems[0]),long(tmpItems[1]))
                    if not runEvent in guids:
                        guids[runEvent] = set()
                    setRunEvent = False
                    continue
                if 'Stream' in tmpLine:
                    tmpItem = tmpLine.split()
                    if len(tmpItem) == 2:
                        tmpToken = tmpItem[1]
                        tmpGUID = tmpItem[0]
                        if tokens is None or tokens == tmpToken:
                            guids[runEvent].add(tmpGUID)
                    continue
            if '0 results found' in tmpOut:
                # add dummy
                guids[None] = None
        except Exception:
            pass
        return guids,command,tmpOut,tmpErr

