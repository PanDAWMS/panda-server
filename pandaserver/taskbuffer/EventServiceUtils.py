import re


# status codes for each event range
ST_ready     = 0
ST_sent      = 1
ST_running   = 2
ST_finished  = 3
ST_cancelled = 4
ST_discarded = 5
ST_done      = 6
ST_failed    = 7
ST_fatal     = 8



# identifiers for specialHandling
esHeader = 'es:'
esToken  = 'eventservice'
esMergeToken = 'esmerge'
singleToken = 'sc'


# encode file info
def encodeFileInfo(lfn,startEvent,endEvent,nEventsPerWorker,maxAttempt=None):
    if maxAttempt == None:
        return '{0}/{1}/{2}/{3}^'.format(lfn,startEvent,endEvent,nEventsPerWorker)
    else:
        return '{0}/{1}/{2}/{3}/{4}^'.format(lfn,startEvent,endEvent,nEventsPerWorker,maxAttempt)



# get header for specialHandling
def getHeaderForES(esIndex):
    return '{0}{1}:'.format(esHeader,esIndex)



# decode file info
def decodeFileInfo(specialHandling):
    eventServiceInfo = {}
    newSpecialHandling = ''
    esIndex = None
    try:
        for tmpItem in specialHandling.split(','):
            if tmpItem.startswith(esHeader):
                tmpItem = re.sub('^'+esHeader,'',tmpItem)
                # look for event service index
                tmpMatch = re.search('^(\d+):',tmpItem)
                if tmpMatch != None:
                    esIndex = tmpMatch.group(1)
                    tmpItem = re.sub('^(\d+):','',tmpItem)
                for esItem in tmpItem.split('^'):
                    if esItem == '':
                        continue
                    esItems = esItem.split('/')
                    maxAttempt = 10
                    if len(esItems) == 3:
                        esLFN,esEvents,esRange = esItems
                        esStartEvent = 0
                    elif len(esItems) == 5:
                        esLFN,esStartEvent,esEndEvent,esRange,maxAttempt = esItems
                        esEvents = long(esEndEvent) - long(esStartEvent) + 1
                    else:
                        esLFN,esStartEvent,esEndEvent,esRange = esItems
                        esEvents = long(esEndEvent) - long(esStartEvent) + 1
                    eventServiceInfo[esLFN] = {'nEvents':long(esEvents),
                                               'startEvent':long(esStartEvent),
                                               'nEventsPerRange':long(esRange),
                                               'maxAttempt':long(maxAttempt)}
                newSpecialHandling += '{0},'.format(esToken)
            else:
                newSpecialHandling += '{0},'.format(tmpItem)
        newSpecialHandling = newSpecialHandling[:-1]
    except:
        newSpecialHandling = specialHandling
    return eventServiceInfo,newSpecialHandling,esIndex



# check if event service job
def isEventServiceJob(job):
    try:
        if job.specialHandling != None and esToken in job.specialHandling.split(','):
            return True
    except:
        pass
    return False



# check if event service merge job
def isEventServiceMerge(job):
    try:
        if job.specialHandling != None and esMergeToken in job.specialHandling.split(','):
            return True
    except:
        pass
    return False



# check if specialHandling for event service
def isEventServiceSH(specialHandling):
    try:
        if specialHandling != None and esToken in specialHandling.split(','):
            return True
    except:
        pass
    return False



# check if specialHandling for event service merge 
def isEventServiceMergeSH(specialHandling):
    try:
        if specialHandling != None and esMergeToken in specialHandling.split(','):
            return True
    except:
        pass
    return False



# set event service merge
def setEventServiceMerge(job):
    try:
        # set flag for merging
        if job.specialHandling == None:
            job.specialHandling = esMergeToken
        else:
            newSpecialHandling = ''
            # remove flag for event service
            for tmpFlag in job.specialHandling.split(','):
                if tmpFlag in ['',esToken]:
                    continue
                newSpecialHandling += tmpFlag
                newSpecialHandling += ','
            newSpecialHandling += esMergeToken
            job.specialHandling = newSpecialHandling
    except:
        pass



# check if specialHandling for single consumer
def isSingleConsumerSH(specialHandling):
    try:
        if specialHandling != None and singleToken in specialHandling.split(','):
            return True
    except:
        pass
    return False



# check if event service job
def isSingleConsumerJob(job):
    return isSingleConsumerSH(job.specialHandling)



# set header for single consumer
def setHeaderForSingleConsumer(specialHandling):
    if specialHandling == None:
        specialHandling = ''
    tokens = specialHandling.split(',')
    while True:
        try:
            tokens.remove('')
        except:
            break
    tokens.append(singleToken)
    return ','.join(tokens)
