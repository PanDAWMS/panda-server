from pandaserver.config import panda_config
from pandaserver.srvcore.CoreUtils import CacheDict

if panda_config.token_authType is None:
    pass
elif panda_config.token_authType == "scitokens":
    import scitokens
else:
    from pandaserver.srvcore.oidc_utils import TokenDecoder

    token_decoder = TokenDecoder()

import traceback

cache_dict = CacheDict()


# decode token
def decode_token(serialized_token, env, tmp_log):
    authenticated = False
    message_str = None
    subprocess_env = {}
    try:
        vo = None
        role = None
        if panda_config.token_authType == "oidc":
            if "HTTP_ORIGIN" in env:
                # two formats, vo:role and vo.role
                vo = env["HTTP_ORIGIN"]
                # only vo.role for auth filename which is a key of auth_vo_dict
                vo_role = vo.replace(":", ".")
            else:
                vo = None
                vo_role = ""
            # only vo.role for auth filename which is a key of auth_vo_dict
            vo_role = vo.replace(":", ".")
            token = token_decoder.deserialize_token(serialized_token, panda_config.auth_config, vo, tmp_log)
            # extract role
            if vo:
                if ":" in token["vo"]:
                    # vo:role
                    vo, role = token["vo"].split(":")
                else:
                    # vo.role
                    vo = token["vo"].split(".")[0]
                    if vo != token["vo"]:
                        role = token["vo"].split(":")[-1]
                        role = role.split(".")[-1]
            # check vo
            if vo not in panda_config.auth_policies:
                message_str = f"Unknown vo : {vo}"
            else:
                # robot
                if vo_role in panda_config.auth_vo_dict and "robot_ids" in panda_config.auth_vo_dict[vo_role]:
                    robot_ids = panda_config.auth_vo_dict[vo_role].get("robot_ids")
                    if isinstance(robot_ids, str):
                        robot_ids = robot_ids.split(",")
                    if not robot_ids:
                        robot_ids = []
                    robot_ids = [i for i in robot_ids if i]
                    if token["sub"] in robot_ids:
                        if "groups" not in token:
                            if role:
                                token["groups"] = [f"{vo}/{role}"]
                            else:
                                token["groups"] = [f"{vo}"]
                        if "name" not in token:
                            token["name"] = f"robot {role}"
                # check role
                if role:
                    if f"{vo}/{role}" not in token["groups"]:
                        message_str = f"Not a member of the {vo}/{role} group"
                    else:
                        subprocess_env["PANDA_OIDC_VO"] = vo
                        subprocess_env["PANDA_OIDC_GROUP"] = role
                        subprocess_env["PANDA_OIDC_ROLE"] = role
                        authenticated = True
                else:
                    for member_string, member_info in panda_config.auth_policies[vo]:
                        if member_string in token["groups"]:
                            subprocess_env["PANDA_OIDC_VO"] = vo
                            subprocess_env["PANDA_OIDC_GROUP"] = member_info["group"]
                            subprocess_env["PANDA_OIDC_ROLE"] = member_info["role"]
                            authenticated = True
                            break
                    if not authenticated:
                        message_str = f"Not a member of the {vo} group"
        else:
            token = scitokens.SciToken.deserialize(serialized_token, audience=panda_config.token_audience)

        # check issuer
        if "iss" not in token:
            message_str = "Issuer is undefined in the token"
        else:
            if panda_config.token_authType == "scitokens":
                items = token.claims()
            else:
                items = token.items()
            for c, v in items:
                subprocess_env[f"PANDA_OIDC_CLAIM_{str(c)}"] = str(v)
            # use sub and scope as DN and FQAN
            if "SSL_CLIENT_S_DN" not in env:
                if "name" in token:
                    subprocess_env["SSL_CLIENT_S_DN"] = " ".join([t[:1].upper() + t[1:].lower() for t in str(token["name"]).split()])
                    if "preferred_username" in token:
                        subprocess_env["SSL_CLIENT_S_DN"] += f"/CN=nickname:{token['preferred_username']}"
                else:
                    subprocess_env["SSL_CLIENT_S_DN"] = str(token["sub"])
                i = 0
                for scope in token.get("scope", "").split():
                    if scope.startswith("role:"):
                        subprocess_env[f"GRST_CRED_AUTH_TOKEN_{i}"] = "VOMS " + str(scope.split(":")[-1])
                        i += 1
                if role:
                    subprocess_env[f"GRST_CRED_AUTH_TOKEN_{i}"] = f"VOMS /{vo}/Role={role}"
                    i += 1
            else:
                # protection against cached decisions that miss x509-related variables due to token+x509 access
                subprocess_env["SSL_CLIENT_S_DN"] = env["SSL_CLIENT_S_DN"]
                for key in env:
                    if key.startswith("GRST_CRED_") or key.startswith("GRST_CONN_"):
                        subprocess_env[key] = env[key]

    except Exception as e:
        message_str = f"Corrupted token. {str(e)}"
    return {"authenticated": authenticated, "message": message_str, "subprocess_env": subprocess_env}


# PanDA request object
class PandaRequest:
    def __init__(self, env, tmp_log):
        # environment
        self.subprocess_env = env
        # header
        self.headers_in = {}
        # authentication
        self.authenticated = True
        # message
        self.message = None

        # content-length
        if "CONTENT_LENGTH" in self.subprocess_env:
            self.headers_in["content-length"] = self.subprocess_env["CONTENT_LENGTH"]

        # tokens
        try:
            if panda_config.token_authType in ["scitokens", "oidc"] and "HTTP_AUTHORIZATION" in env:
                serialized_token = env["HTTP_AUTHORIZATION"].split()[1]
                decision_key = f"""{serialized_token} : {env.get("HTTP_ORIGIN", None)} : {env.get("SSL_CLIENT_S_DN", None)}"""
                cached_decision = cache_dict.get(decision_key, tmp_log, decode_token, serialized_token, env, tmp_log)
                self.authenticated = cached_decision["authenticated"]
                self.message = cached_decision["message"]
                if self.message:
                    tmp_log.error(f"""{self.message} - Origin: {env.get("HTTP_ORIGIN", None)}, Token: {env["HTTP_AUTHORIZATION"]}""")
                self.subprocess_env.update(cached_decision["subprocess_env"])
        except Exception as e:
            self.message = f"Failed to instantiate reqeust object. {str(e)}"
            tmp_log.error(
                f"""{self.message} - Origin: {env.get("HTTP_ORIGIN", None)}, Token: {env.get("HTTP_AUTHORIZATION", None)}\n{traceback.format_exc()}"""
            )

    # get remote host
    def get_remote_host(self):
        if "REMOTE_HOST" in self.subprocess_env:
            return self.subprocess_env["REMOTE_HOST"]
        return ""

    # accept json
    def acceptJson(self):
        try:
            if "HTTP_ACCEPT" in self.subprocess_env:
                return "application/json" in self.subprocess_env["HTTP_ACCEPT"]
        except Exception:
            pass
        return False
