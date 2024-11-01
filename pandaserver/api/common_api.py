MESSAGE_SSL = "SSL secure connection is required"
MESSAGE_PROD_ROLE = "production or pilot role required"
MESSAGE_TASK_ID = "jediTaskID must be an integer"
MESSAGE_DATABASE = "database error in the PanDA server"
MESSAGE_JSON = "failed to load JSON"


# get FQANs
def _getFQAN(req):
    fqans = []
    for tmp_key in req.subprocess_env:
        tmp_value = req.subprocess_env[tmp_key]
        # Scan VOMS attributes
        # compact style
        if tmp_key.startswith("GRST_CRED_") and tmp_value.startswith("VOMS"):
            fqan = tmp_value.split()[-1]
            fqans.append(fqan)

        # old style
        elif tmp_key.startswith("GRST_CONN_"):
            tmp_items = tmp_value.split(":")
            if len(tmp_items) == 2 and tmp_items[0] == "fqan":
                fqans.append(tmp_items[-1])

    return fqans


# get DN
def _getDN(req):
    real_dn = ""
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        # remove redundant CN
        real_dn = CoreUtils.get_bare_dn(req.subprocess_env["SSL_CLIENT_S_DN"], keep_proxy=True)
    return real_dn


# check role
def _has_production_role(req):
    # check DN
    user = _getDN(req)
    for sdn in panda_config.production_dns:
        if sdn in user:
            return True
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in [
            "/atlas/usatlas/Role=production",
            "/atlas/Role=production",
            "^/[^/]+/Role=production",
        ]:
            if fqan.startswith(rolePat):
                return True
            if re.search(rolePat, fqan):
                return True
    return False


# get primary working group with prod role
def _getWGwithPR(req):
    try:
        fqans = _getFQAN(req)
        for fqan in fqans:
            tmpMatch = re.search("/[^/]+/([^/]+)/Role=production", fqan)
            if tmpMatch is not None:
                # ignore usatlas since it is used as atlas prod role
                tmpWG = tmpMatch.group(1)
                if tmpWG not in ["", "usatlas"]:
                    return tmpWG.split("-")[-1].lower()
    except Exception:
        pass
    return None


# security check
def isSecure(req):
    # check security
    if not Protocol.isSecure(req):
        return False
    # disable limited proxy
    if "/CN=limited proxy" in req.subprocess_env["SSL_CLIENT_S_DN"]:
        _logger.warning(f"access via limited proxy : {req.subprocess_env['SSL_CLIENT_S_DN']}")
        return False
    return True


def require_secure(func):
    @wraps(func)
    def wrapper(req, *args, **kwargs):
        if not is_secure(req):
            # Print the function name and a custom message
            _logger.error(f"'{func.__name__}': {MESSAGE_SSL}")
            return json.dumps((False, MESSAGE_SSL))
        return func(req, *args, **kwargs)

    return wrapper


def require_production_role(func):
    @wraps(func)
    def wrapper(req, *args, **kwargs):
        if not _has_production_role(req):
            return WrappedPickle.dumps((False, MESSAGE_PROD_ROLE))
        return func(req, *args, **kwargs)

    return wrapper


def validate_types(type_mapping):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for param, expected_type in type_mapping.items():
                # Find the argument either in args or kwargs
                if param in kwargs:
                    value = kwargs[param]
                else:
                    # Get the index of the argument in args based on func's signature
                    arg_names = func.__code__.co_varnames
                    param_index = arg_names.index(param)
                    value = args[param_index]

                # Try to convert and validate type
                try:
                    converted_value = expected_type(value)
                except (ValueError, TypeError):
                    return json.dumps((False, f"{param} must be {expected_type.__name__}"))

                # Update the args or kwargs with the converted value
                if param in kwargs:
                    kwargs[param] = converted_value
                else:
                    args = list(args)
                    args[param_index] = converted_value

            return func(*args, **kwargs)

        return wrapper

    return decorator
