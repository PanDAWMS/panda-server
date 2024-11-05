# force avalanche for task
def avalancheTask(req, jediTaskID):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_role = _has_production_role(req)
    # only prod managers can use this method
    if not is_production_role:
        return WrappedPickle.dumps((False, "production role required"))
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    ret = userIF.avalancheTask(jediTaskID, user, is_production_role)
    return WrappedPickle.dumps(ret)


# release task
def release_task(req, jedi_task_id):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    prod_role = _has_production_role(req)
    # only prod managers can use this method
    if not prod_role:
        return json.dumps((False, "production role required"))
    # check jediTaskID
    try:
        jedi_task_id = int(jedi_task_id)
    except Exception:
        return json.dumps((False, MESSAGE_TASK_ID))
    ret = userIF.send_command_to_task(jedi_task_id, user, prod_role, "release")
    return json.dumps(ret)


# kill unfinished jobs
def killUnfinishedJobs(req, jediTaskID, code=None, useMailAsID=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        user = _getDN(req)
    # check role
    is_production_manager = False
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ["/atlas/usatlas/Role=production", "/atlas/Role=production"]:
            if fqan.startswith(rolePat):
                is_production_manager = True
                break
        # escape
        if is_production_manager:
            break
    # use email address as ID
    useMailAsID = resolve_true(useMailAsID)
    # hostname
    host = req.get_remote_host()
    # get PandaIDs
    ids = userIF.getPandaIDsWithTaskID(jediTaskID)
    # kill
    return userIF.killJobs(ids, user, host, code, is_production_manager, useMailAsID, fqans)


# change modificationTime for task
def changeTaskModTimePanda(req, jediTaskID, diffValue):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))

    # check role
    is_production_role = _has_production_role(req)
    # only prod managers can use this method
    if not is_production_role:
        return WrappedPickle.dumps((False, MESSAGE_PROD_ROLE))
    # check jediTaskID
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    try:
        diffValue = int(diffValue)
        attrValue = datetime.datetime.now() + datetime.timedelta(hours=diffValue)
    except Exception:
        return WrappedPickle.dumps((False, f"failed to convert {diffValue} to time diff"))
    ret = userIF.changeTaskAttributePanda(jediTaskID, "modificationTime", attrValue)
    return WrappedPickle.dumps((ret, None))


# get PandaIDs with TaskID
def getPandaIDsWithTaskID(req, jediTaskID):
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    idsStr = userIF.getPandaIDsWithTaskID(jediTaskID)
    # deserialize
    ids = WrappedPickle.loads(idsStr)

    return WrappedPickle.dumps(ids)


# reactivate Task
def reactivateTask(req, jediTaskID, keep_attempt_nr=None, trigger_job_generation=None):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))
    # check role
    is_production_manager = _has_production_role(req)
    if not is_production_manager:
        msg = "production role is required"
        _logger.error(f"reactivateTask: {msg}")
        return WrappedPickle.dumps((False, msg))
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    keep_attempt_nr = resolve_true(keep_attempt_nr)
    trigger_job_generation = resolve_true(trigger_job_generation)

    ret = userIF.reactivateTask(jediTaskID, keep_attempt_nr, trigger_job_generation)

    return WrappedPickle.dumps(ret)


# get task status
def getTaskStatus(req, jediTaskID):
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    ret = userIF.getTaskStatus(jediTaskID)
    return WrappedPickle.dumps(ret)


# reassign share
def reassignShare(req, jedi_task_ids_pickle, share, reassign_running):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, MESSAGE_SSL))

    # check role
    prod_role = _has_production_role(req)
    if not prod_role:
        return WrappedPickle.dumps((False, MESSAGE_PROD_ROLE))

    jedi_task_ids = WrappedPickle.loads(jedi_task_ids_pickle)
    _logger.debug(f"reassignShare: jedi_task_ids: {jedi_task_ids}, share: {share}, reassign_running: {reassign_running}")

    if not ((isinstance(jedi_task_ids, list) or (isinstance(jedi_task_ids, tuple)) and isinstance(share, str))):
        return WrappedPickle.dumps((False, "jedi_task_ids must be tuple/list and share must be string"))

    ret = userIF.reassignShare(jedi_task_ids, share, reassign_running)
    return WrappedPickle.dumps(ret)


# get taskParamsMap with TaskID
def getTaskParamsMap(req, jediTaskID):
    try:
        jediTaskID = int(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, MESSAGE_TASK_ID))
    ret = userIF.getTaskParamsMap(jediTaskID)
    return WrappedPickle.dumps(ret)
