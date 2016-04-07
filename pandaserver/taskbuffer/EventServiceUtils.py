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
singleConsumerType = {'runonce':  '1',
                      'storeonce':'2'}
dynamicNumEventsToken = 'dy'

# values for job.eventService
esJobFlagNumber = 1
esMergeJobFlagNumber = 2

# relation type for jobsets
relationTypeJS_ID = 'jobset_id'
relationTypeJS_Retry = 'jobset_retry'
relationTypesForJS = [relationTypeJS_ID,relationTypeJS_Retry]



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
        # set ES flag
        job.eventService = esMergeJobFlagNumber
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



# check if specialHandling for job cloning
def isJobCloningSH(specialHandling):
    try:
        if specialHandling != None:
            for token in specialHandling.split(','):
                if singleToken == token.split(':')[0]:
                    return True
    except:
        pass
    return False



# check if event service job
def isJobCloningJob(job):
    return isJobCloningSH(job.specialHandling)



# set header for job cloning
def setHeaderForJobCloning(specialHandling,scType):
    if specialHandling == None:
        specialHandling = ''
    tokens = specialHandling.split(',')
    while True:
        try:
            tokens.remove('')
        except:
            break
    if scType in singleConsumerType.values():
        tokens.append('{0}:{1}'.format(singleToken,scType))
    return ','.join(tokens)



# get consumer type
def getJobCloningType(job):
    if job.specialHandling != None:
        for token in job.specialHandling.split(','):
            if singleToken == token.split(':')[0]:
                for tmpKey,tmpVal in singleConsumerType.iteritems():
                    if tmpVal == token.split(':')[-1]:
                        return tmpKey
    return ''



# get consumer value
def getJobCloningValue(scType):
    if scType in singleConsumerType:
        return singleConsumerType[scType]
    return ''



# set header for dynamic number of events
def setHeaderForDynNumEvents(specialHandling):
    if specialHandling == None:
        specialHandling = ''
    tokens = specialHandling.split(',')
    while True:
        try:
            tokens.remove('')
        except:
            break
    tokens.append(dynamicNumEventsToken)
    return ','.join(tokens)



# check if specialHandling for dynamic number of events
def isDynNumEventsSH(specialHandling):
    try:
        if specialHandling != None:
            if dynamicNumEventsToken in specialHandling.split(','):
                return True
    except:
        pass
    return False
