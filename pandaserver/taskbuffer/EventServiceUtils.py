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
ST_merged    = 9



# identifiers for specialHandling
esHeader = 'es:'
singleConsumerType = {'runonce':  '1',
                      'storeonce':'2'}

# tags for special handling. check JobSpec._tagForSH for duplication
esToken                 = 'eventservice'
esMergeToken            = 'esmerge'
dynamicNumEventsToken   = 'dy'
mergeAtOsToken          = 'mo'
resurrectConsumersToken = 'rs'
singleToken             = 'sc'


# values for job.eventService
esJobFlagNumber = 1
esMergeJobFlagNumber = 2
jobCloningFlagNumber = 3
jumboJobFlagNumber = 4
coJumboJobFlagNumber = 5


# values for event.is_jumbo
eventTableIsJumbo = 1


# siteid for waiting co-jumbo jobs
siteIdForWaitingCoJumboJobs = 'WAITING_CO_JUMBO'


# relation type for jobsets
relationTypeJS_ID = 'jobset_id'
relationTypeJS_Retry = 'jobset_retry'
relationTypeJS_Map = 'jobset_map'
relationTypesForJS = [relationTypeJS_ID, relationTypeJS_Retry, relationTypeJS_Map]


# suffix for ES dataset and files to register to DDM
esSuffixDDM = '.events'
esScopeDDM = 'transient'
esRegStatus = 'esregister'

# default max number of ES job attempt
defMaxAttemptEsJob = 3


# encode file info
def encodeFileInfo(lfn,startEvent,endEvent,nEventsPerWorker,
                   maxAttempt=None,firstOffset=None,firstEvent=None):
    if maxAttempt == None:
        maxAttempt = 10
    if firstOffset == None:
        return '{0}/{1}/{2}/{3}/{4}^'.format(lfn,startEvent,endEvent,nEventsPerWorker,maxAttempt)
    else:
        try:
            totalOffset = firstEvent - firstOffset
        except:
            totalOffset = 0
        return '{0}/{1}/{2}/{3}/{4}/{5}^'.format(lfn,startEvent,endEvent,nEventsPerWorker,
                                                 maxAttempt,totalOffset)



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
                    esOffset = 0
                    if len(esItems) == 3:
                        esLFN,esEvents,esRange = esItems
                        esStartEvent = 0
                    elif len(esItems) == 5:
                        esLFN,esStartEvent,esEndEvent,esRange,maxAttempt = esItems
                        esEvents = long(esEndEvent) - long(esStartEvent) + 1
                    elif len(esItems) == 6:
                        esLFN,esStartEvent,esEndEvent,esRange,maxAttempt,esOffset \
                            = esItems
                        esEvents = long(esEndEvent) - long(esStartEvent) + 1
                    else:
                        esLFN,esStartEvent,esEndEvent,esRange = esItems
                        esEvents = long(esEndEvent) - long(esStartEvent) + 1
                    eventServiceInfo[esLFN] = {'nEvents':long(esEvents),
                                               'startEvent':long(esStartEvent),
                                               'nEventsPerRange':long(esRange),
                                               'maxAttempt':long(maxAttempt),
                                               'offset':long(esOffset)}
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
        # set gshare to express
        job.gshare = 'Express'
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



# remove event service header
def removeHeaderForES(job):
    if job.specialHandling != None:
        items = job.specialHandling.split(',')
        newItems = []
        for item in items:
            if re.search('^'+esHeader+'.+',item) == None:
                newItems.append(item)
    job.specialHandling = ','.join(newItems)



# check if jumbo job
def isJumboJob(job):
    return job.eventService == jumboJobFlagNumber



# check if cooperative with jumbo job
def isCoJumboJob(job):
    return job.eventService == coJumboJobFlagNumber



# set header for merge at OS
def setHeaderForMergeAtOS(specialHandling):
    if specialHandling == None:
        specialHandling = ''
    tokens = specialHandling.split(',')
    while True:
        try:
            tokens.remove('')
        except:
            break
    if mergeAtOsToken not in tokens:
        tokens.append(mergeAtOsToken)
    return ','.join(tokens)



# check if specialHandling for merge at OS
def isMergeAtOS(specialHandling):
    try:
        if specialHandling != None:
            if mergeAtOsToken in specialHandling.split(','):
                return True
    except:
        pass
    return False



# get dataset name for event service
def getEsDatasetName(taskID):
    esDataset = '{0}:{1}{2}'.format(esScopeDDM, taskID, esSuffixDDM)
    return esDataset



# set header to resurrect consumers
def setHeaderToResurrectConsumers(specialHandling):
    if specialHandling == None:
        specialHandling = ''
    tokens = specialHandling.split(',')
    while True:
        try:
            tokens.remove('')
        except:
            break
    if resurrectConsumersToken not in tokens:
        tokens.append(resurrectConsumersToken)
    return ','.join(tokens)



# check if specialHandling to resurrect consumers
def isResurrectConsumers(specialHandling):
    try:
        if specialHandling is not None:
            if resurrectConsumersToken in specialHandling.split(','):
                return True
    except:
        pass
    return False
