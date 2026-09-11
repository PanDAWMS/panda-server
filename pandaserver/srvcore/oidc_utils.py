import base64
import datetime
from threading import Lock
from typing import Any

import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from jwt.exceptions import InvalidTokenError
from pandacommon.pandautils.PandaUtils import naive_utcnow


def decode_value(val: str | bytes) -> int:
    if isinstance(val, str):
        val = val.encode()
    decoded = base64.urlsafe_b64decode(val + b"==")
    return int.from_bytes(decoded, "big")


def rsa_pem_from_jwk(jwk: dict[str, Any]) -> bytes:
    public_num = RSAPublicNumbers(n=decode_value(jwk["n"]), e=decode_value(jwk["e"]))
    public_key = public_num.public_key(default_backend())
    pem: bytes = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return pem


def get_jwk(kid: str, jwks: dict[str, Any]) -> dict[str, Any]:
    keys: list[dict[str, Any]] = jwks.get("keys", [])
    for jwk in keys:
        if jwk.get("kid") == kid:
            return jwk
    raise InvalidTokenError(f"JWK not found for kid={kid}")


# token decoder
class TokenDecoder:
    # constructor
    def __init__(self, refresh_interval: int = 10) -> None:
        self.lock = Lock()
        self.data: dict[str, dict[str, Any]] = {}
        self.refresh_interval = refresh_interval

    # get cached data
    def get_data(self, url: str, log_stream: Any) -> Any:
        try:
            with self.lock:
                if url not in self.data or naive_utcnow() - self.data[url]["last_update"] > datetime.timedelta(minutes=self.refresh_interval):
                    log_stream.debug(f"to refresh {url}")
                    tmp_data = requests.get(url).json()
                    log_stream.debug("refreshed")
                    self.data[url] = {
                        "data": tmp_data,
                        "last_update": naive_utcnow(),
                    }
                return self.data[url]["data"]
        except Exception as e:
            log_stream.error(f"failed to refresh with {str(e)}")
            raise

    # decode and verify JWT token
    def deserialize_token(
        self, token: str, auth_config: dict[str, Any], vo: str | None, log_stream: Any, legacy_token_issuers: list[str] | None
    ) -> dict[str, Any]:
        try:
            # check audience
            unverified = jwt.decode(token, verify=False, options={"verify_signature": False})
            client_id = None
            audience = None
            if "aud" in unverified:
                audience = unverified["aud"]
                # client ID from aud of ID token
                if audience in auth_config:
                    client_id = audience
            if not client_id:
                # client ID from access token including ID info with device code flow
                client_id = unverified.get("client_id")
                if client_id and client_id not in auth_config:
                    client_id = None
            if not client_id:
                # client ID from sub of access token with client_credentials flow
                client_id = unverified["sub"]
                if client_id and client_id not in auth_config:
                    client_id = None
            if not client_id:
                raise jwt.exceptions.InvalidTokenError("cannot extract client_id from token")
            discovery_endpoint = auth_config[client_id]["oidc_config_url"]
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
            if legacy_token_issuers:
                issuers = list(dict.fromkeys([issuer] + legacy_token_issuers))
            else:
                issuers = [issuer]
            decoded: dict[str, Any] | None = None
            err_msg = None
            for tmp_issuer in issuers:
                try:
                    decoded = jwt.decode(
                        token,
                        public_key,
                        verify=True,
                        algorithms="RS256",
                        audience=audience,
                        issuer=tmp_issuer,
                    )
                    break
                except jwt.exceptions.InvalidIssuerError as e:
                    err_msg = str(e)
            if not decoded:
                raise jwt.exceptions.InvalidTokenError(f"failed to decode: {err_msg}")
            if vo is not None:
                decoded["vo"] = vo
            else:
                decoded["vo"] = auth_config[client_id]["vo"]
            # client ID
            decoded["extracted_client_id"] = client_id
            return decoded
        except Exception:
            raise


# get an access token with client_credentials flow
def get_access_token(token_endpoint: str, client_id: str, client_secret: str, scope: str | None = None, timeout: int = 180) -> tuple[bool, str]:
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
