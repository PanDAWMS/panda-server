import base64
import datetime
from threading import Lock

import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from jwt.exceptions import InvalidTokenError


def decode_value(val):
    if isinstance(val, str):
        val = val.encode()
    decoded = base64.urlsafe_b64decode(val + b"==")
    return int.from_bytes(decoded, "big")


def rsa_pem_from_jwk(jwk):
    public_num = RSAPublicNumbers(n=decode_value(jwk["n"]), e=decode_value(jwk["e"]))
    public_key = public_num.public_key(default_backend())
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return pem


def get_jwk(kid, jwks):
    for jwk in jwks.get("keys", []):
        if jwk.get("kid") == kid:
            return jwk
    raise InvalidTokenError("JWK not found for kid={0}".format(kid, str(jwks)))


# token decoder
class TokenDecoder:
    # constructor
    def __init__(self, refresh_interval=10):
        self.lock = Lock()
        self.data = {}
        self.refresh_interval = refresh_interval

    # get cached data
    def get_data(self, url, log_stream):
        try:
            with self.lock:
                if url not in self.data or datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - self.data[url][
                    "last_update"
                ] > datetime.timedelta(minutes=self.refresh_interval):
                    log_stream.debug(f"to refresh {url}")
                    tmp_data = requests.get(url).json()
                    log_stream.debug("refreshed")
                    self.data[url] = {
                        "data": tmp_data,
                        "last_update": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
                    }
                return self.data[url]["data"]
        except Exception as e:
            log_stream.error(f"failed to refresh with {str(e)}")
            raise

    # decode and verify JWT token
    def deserialize_token(self, token, auth_config, vo, log_stream):
        try:
            # check audience
            unverified = jwt.decode(token, verify=False, options={"verify_signature": False})
            conf_key = None
            audience = None
            if "aud" in unverified:
                audience = unverified["aud"]
                if audience in auth_config:
                    conf_key = audience
            if not conf_key:
                # use sub as config key for access token
                conf_key = unverified["sub"]
            discovery_endpoint = auth_config[conf_key]["oidc_config_url"]
            # decode headers
            headers = jwt.get_unverified_header(token)
            # get key id
            if headers is None or "kid" not in headers:
                raise jwt.exceptions.InvalidTokenError("cannot extract kid from headers")
            kid = headers["kid"]
            # retrieve OIDC configuration and JWK set
            oidc_config = self.get_data(discovery_endpoint, log_stream)
            jwks = self.get_data(oidc_config["jwks_uri"], log_stream)
            # get JWK and public key
            jwk = get_jwk(kid, jwks)
            public_key = rsa_pem_from_jwk(jwk)
            # decode token only with RS256
            if unverified["iss"] and unverified["iss"] != oidc_config["issuer"] and oidc_config["issuer"].startswith(unverified["iss"]):
                # iss is missing the last '/' in access tokens
                issuer = unverified["iss"]
            else:
                issuer = oidc_config["issuer"]
            decoded = jwt.decode(
                token,
                public_key,
                verify=True,
                algorithms="RS256",
                audience=audience,
                issuer=issuer,
            )
            if vo is not None:
                decoded["vo"] = vo
            else:
                decoded["vo"] = auth_config[conf_key]["vo"]
            return decoded
        except Exception:
            raise


# get an access token with client_credentials flow
def get_access_token(token_endpoint: str, client_id: str, client_secret: str, scope: str = None, timeout: int = 180) -> tuple[bool, str]:
    """
    Get an access token with client_credentials flow

    :param token_endpoint: URL for token request
    :param client_id: client ID
    :param client_secret: client secret
    :param scope: space separated string of scopes
    :param timeout: timeout in seconds

    :return: (True, access_token) or (False, error_str)
    """
    try:
        token_request = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        if scope:
            token_request["scope"] = scope
        token_response = requests.post(token_endpoint, data=token_request, timeout=timeout)
        token_response.raise_for_status()
        return True, token_response.json()["access_token"]
    except Exception as e:
        error_str = f"failed to get access token with {str(e)}"
        return False, error_str
