import os
import subprocess


class eventLookupClientEI:

    def __init__(self):
        pass


    def doLookup(self,runEvtList,stream=None,tokens=None,amitag=None):
        command = 'java -jar ' + \
            os.getenv('EIDIR', '/afs/cern.ch/sw/lcg/external/Java/TagConvertor/head/share') + \
            '/lib/EventLookup.exe.jar '
        command += "-e '"
        for runEvt in runEvtList:
            tmpStr = '{0:08d} {1:09d},'.format(long(runEvt[0]),long(runEvt[1]))
            command += tmpStr
        command = command[:-1]
        command += "' "
        if not stream in [None,'']:
            command += "-s {0} ".format(stream)
        if not amitag in [None,'']:
            command += "-p {0} ".format(amitag)
        command += r"""-filter 'String RunNumber_EventNumber + "\n" + guids()' """
        p = subprocess.Popen(command, stdout=subprocess.PIPE,shell=True)
        tmpOut,tmpErr = p.communicate()
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
                        if tokens == None or tokens == tmpToken:
                            guids[runEvent].add(tmpGUID)
                    continue
            if '0 results found' in tmpOut:
                # add dummy
                guids[None] = None
        except:
            pass
        return guids,command,tmpOut,tmpErr

