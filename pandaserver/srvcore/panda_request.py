from pandaserver.config import panda_config

if panda_config.token_authType is None:
    pass
elif panda_config.token_authType == "scitokens":
    import scitokens
else:
    from pandaserver.srvcore.oidc_utils import TokenDecoder

    token_decoder = TokenDecoder()

import traceback


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
                role = None
                if panda_config.token_authType == "oidc":
                    self.authenticated = False
                    if "HTTP_ORIGIN" in env:
                        vo = env["HTTP_ORIGIN"]
                        vo_group = vo.replace(":", ".")
                    else:
                        vo = None
                        vo_group = ""
                    token = token_decoder.deserialize_token(serialized_token, panda_config.auth_config, vo, tmp_log)
                    # extract role
                    if vo:
                        vo = token["vo"].split(":")[0]
                        vo = vo.split(".")[0]
                        if vo != token["vo"]:
                            role = token["vo"].split(":")[-1]
                            role = role.split(".")[-1]
                    # check vo
                    if vo not in panda_config.auth_policies:
                        self.message = f"Unknown vo : {vo}"
                        tmp_log.error(f"{self.message} - {env['HTTP_AUTHORIZATION']}")
                    else:
                        # robot
                        if vo_group in panda_config.auth_vo_dict and "robot_ids" in panda_config.auth_vo_dict[vo_group]:
                            robot_ids = [i for i in panda_config.auth_vo_dict[vo_group].get("robot_ids").split(",") if i]
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
                                self.message = f"Not a member of the {vo}/{role} group"
                                tmp_log.error(f"{self.message} - {env['HTTP_AUTHORIZATION']}")
                            else:
                                self.subprocess_env["PANDA_OIDC_VO"] = vo
                                self.subprocess_env["PANDA_OIDC_GROUP"] = role
                                self.subprocess_env["PANDA_OIDC_ROLE"] = role
                                self.authenticated = True
                        else:
                            for member_string, member_info in panda_config.auth_policies[vo]:
                                if member_string in token["groups"]:
                                    self.subprocess_env["PANDA_OIDC_VO"] = vo
                                    self.subprocess_env["PANDA_OIDC_GROUP"] = member_info["group"]
                                    self.subprocess_env["PANDA_OIDC_ROLE"] = member_info["role"]
                                    self.authenticated = True
                                    break
                            if not self.authenticated:
                                self.message = f"Not a member of the {vo} group"
                                tmp_log.error(f"{self.message} - {env['HTTP_AUTHORIZATION']}")
                else:
                    token = scitokens.SciToken.deserialize(serialized_token, audience=panda_config.token_audience)

                # check issuer
                if "iss" not in token:
                    self.message = "Issuer is undefined in the token"
                    tmp_log.error(self.message)
                else:
                    if panda_config.token_authType == "scitokens":
                        items = token.claims()
                    else:
                        items = token.items()
                    for c, v in items:
                        self.subprocess_env[f"PANDA_OIDC_CLAIM_{str(c)}"] = str(v)
                    # use sub and scope as DN and FQAN
                    if "SSL_CLIENT_S_DN" not in self.subprocess_env:
                        if "name" in token:
                            self.subprocess_env["SSL_CLIENT_S_DN"] = " ".join([t[:1].upper() + t[1:].lower() for t in str(token["name"]).split()])
                            if "preferred_username" in token:
                                self.subprocess_env["SSL_CLIENT_S_DN"] += f"/CN=nickname:{token['preferred_username']}"
                        else:
                            self.subprocess_env["SSL_CLIENT_S_DN"] = str(token["sub"])
                        i = 0
                        for scope in token.get("scope", "").split():
                            if scope.startswith("role:"):
                                self.subprocess_env[f"GRST_CRED_AUTH_TOKEN_{i}"] = "VOMS " + str(scope.split(":")[-1])
                                i += 1
                        if role:
                            self.subprocess_env[f"GRST_CRED_AUTH_TOKEN_{i}"] = f"VOMS /{vo}/Role={role}"
                            i += 1

        except Exception as e:
            self.message = f"Corrupted token. {str(e)}"
            tmp_log.debug(f"Origin: {env.get('HTTP_ORIGIN', None)}, Token: {env.get('HTTP_AUTHORIZATION', None)}\n{traceback.format_exc()}")

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
