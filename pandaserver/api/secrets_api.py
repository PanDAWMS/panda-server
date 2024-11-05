class SecretsAPI:
    def __init__(self):
        self.task_buffer = None

    # initialize
    def init(self, task_buffer):
        self.task_buffer = task_buffer


# set user secret
def set_user_secret(req, key=None, value=None):
    tmp_log = LogWrapper(_logger, f"set_user_secret-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    # get owner
    dn = req.subprocess_env.get("SSL_CLIENT_S_DN")
    if not dn:
        tmp_message = "SSL_CLIENT_S_DN is missing in HTTP request"
        tmp_log.error(tmp_message)
        return json.dumps((False, tmp_message))
    owner = clean_user_id(dn)
    return json.dumps(userIF.set_user_secret(owner, key, value))


# get user secrets
def get_user_secrets(req, keys=None, get_json=None):
    tmp_log = LogWrapper(_logger, f"get_user_secrets-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    # get owner
    dn = req.subprocess_env.get("SSL_CLIENT_S_DN")
    get_json = resolve_true(get_json)

    if not dn:
        tmp_message = "SSL_CLIENT_S_DN is missing in HTTP request"
        tmp_log.error(tmp_message)
        return json.dumps((False, tmp_message))
    owner = clean_user_id(dn)
    return json.dumps(userIF.get_user_secrets(owner, keys, get_json))
