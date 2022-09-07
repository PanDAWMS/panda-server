import jwt
import base64
import requests
import datetime
from threading import Lock
from jwt.exceptions import InvalidTokenError
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def decode_value(val):
    if isinstance(val, str):
        val = val.encode()
    decoded = base64.urlsafe_b64decode(val + b'==')
    return int.from_bytes(decoded, 'big')


def rsa_pem_from_jwk(jwk):
    public_num = RSAPublicNumbers(n=decode_value(jwk['n']),
                               e=decode_value(jwk['e']))
    public_key = public_num.public_key(default_backend())
    pem = public_key.public_bytes(encoding=serialization.Encoding.PEM,
                                  format=serialization.PublicFormat.SubjectPublicKeyInfo)
    return pem


def get_jwk(kid, jwks):
    for jwk in jwks.get('keys', []):
        if jwk.get('kid') == kid:
            return jwk
    raise InvalidTokenError('JWK not found for kid={0}'.format(kid, str(jwks)))


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
                if url not in self.data or \
                        datetime.datetime.utcnow() - self.data[url]['last_update'] > \
                        datetime.timedelta(minutes=self.refresh_interval):
                    log_stream.debug('to refresh {}'.format(url))
                    tmp_data = requests.get(url).json()
                    log_stream.debug('refreshed')
                    self.data[url] = {'data': tmp_data, 'last_update': datetime.datetime.utcnow()}
                return self.data[url]['data']
        except Exception as e:
            log_stream.error('failed to refresh with {}'.format(str(e)))
            raise

    # decode and verify JWT token
    def deserialize_token(self, token, auth_config, vo, log_stream):
        try:
            # check audience
            unverified = jwt.decode(token, verify=False, options={"verify_signature": False})
            conf_key = None
            audience = None
            if 'aud' in unverified:
                audience = unverified['aud']
                if audience in auth_config:
                    conf_key = audience
            if not conf_key:
                # use sub as config key for access token
                conf_key = unverified['sub']
            discovery_endpoint = auth_config[conf_key]['oidc_config_url']
            # decode headers
            headers = jwt.get_unverified_header(token)
            # get key id
            if headers is None or 'kid' not in headers:
                raise jwt.exceptions.InvalidTokenError('cannot extract kid from headers')
            kid = headers['kid']
            # retrieve OIDC configuration and JWK set
            oidc_config = self.get_data(discovery_endpoint, log_stream)
            jwks = self.get_data(oidc_config['jwks_uri'], log_stream)
            # get JWK and public key
            jwk = get_jwk(kid, jwks)
            public_key = rsa_pem_from_jwk(jwk)
            # decode token only with RS256
            if unverified['iss'] and unverified['iss'] != oidc_config['issuer'] and \
                    oidc_config['issuer'].startswith(unverified['iss']):
                # iss is missing the last '/' in access tokens
                issuer = unverified['iss']
            else:
                issuer = oidc_config['issuer']
            decoded = jwt.decode(token, public_key, verify=True, algorithms='RS256',
                                 audience=audience, issuer=issuer)
            if vo is not None:
                decoded['vo'] = vo
            else:
                decoded['vo'] = auth_config[conf_key]['vo']
            return decoded
        except Exception:
            raise
