import jwt
import base64
import requests
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


# decode and verify JWT token
def deserialize_token(token, auth_config, vo):
    try:
        # check audience
        unverified = jwt.decode(token, verify=False, options={"verify_signature": False})
        audience = unverified['aud']
        discovery_endpoint = auth_config[audience]['oidc_config_url']
        # decode headers
        headers = jwt.get_unverified_header(token)
        # get key id
        if headers is None or 'kid' not in headers:
            raise jwt.exceptions.InvalidTokenError('cannot extract kid from headers')
        kid = headers['kid']
        # retrieve OIDC configuration and and JWK set
        oidc_config = requests.get(discovery_endpoint).json()
        jwks = requests.get(oidc_config['jwks_uri']).json()
        # get JWK and public key
        jwk = get_jwk(kid, jwks)
        public_key = rsa_pem_from_jwk(jwk)
        # decode token only with RS256
        decoded = jwt.decode(token, public_key, verify=True, algorithms='RS256',
                             audience=audience, issuer=oidc_config['issuer'])
        if vo is not None:
            decoded['vo'] = vo
        else:
            decoded['vo'] = auth_config[audience]['vo']
        return decoded
    except Exception:
        raise
