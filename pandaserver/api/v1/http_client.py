"""
client methods
"""

import os
from typing import Any

import requests
from pandacommon.pandautils.net_utils import replace_hostname_in_url_randomly

# PanDA server configuration
base_url = os.environ.get("PANDA_URL", "http://pandaserver.cern.ch:25080/server/panda")
base_url_ssl = os.environ.get("PANDA_URL_SSL", "https://pandaserver.cern.ch:25443/server/panda")

api_url = os.environ.get("PANDA_API_URL", "http://pandaserver.cern.ch:25080/api/v1")
api_url_ssl = os.environ.get("PANDA_API_URL_SSL", "https://pandaserver.cern.ch:25443/api/v1")

# exit code
EC_Failed = 255


def is_https(url: str) -> bool:
    # check if https is used
    return url.startswith("https://")


class HttpClient:
    def __init__(self) -> None:
        # verification of the host certificate
        if "PANDA_VERIFY_HOST" in os.environ and os.environ["PANDA_VERIFY_HOST"] == "off":
            self.verifyHost = False
        else:
            self.verifyHost = True

        # request a compressed response
        self.compress = True

        # OIDC
        self.oidc = os.getenv("PANDA_AUTH") == "oidc"
        self.auth_vo = os.getenv("PANDA_AUTH_VO") if self.oidc else None
        self.id_token = os.getenv("PANDA_AUTH_ID_TOKEN") if self.oidc else None
        if self.id_token and self.id_token.startswith("file:"):
            with open(self.id_token[5:], "r") as f:
                self.id_token = f.read().strip()

        # SSL cert/key
        self.ssl_certificate = self._x509() if not self.oidc else None
        self.ssl_key = self._x509() if not self.oidc else None

    # the proxy path, or "" when none was found -- which requests reads as no client cert
    def _x509(self) -> str:
        # retrieve the X509_USER_PROXY from the environment variables
        try:
            return os.environ["X509_USER_PROXY"]
        except Exception:
            pass

        # look for the default place
        x509 = f"/tmp/x509up_u{os.getuid()}"
        if os.access(x509, os.R_OK):
            return x509

        # no valid proxy certificate
        print("No valid grid proxy certificate found")
        return ""

    def _prepare_url(self, url: str) -> tuple[str, bool]:
        """Modify URL with HTTPS check and hostname replacement."""
        use_https = is_https(url)
        if "PANDA_BEHIND_REAL_LB" in os.environ:
            modified_url = url
        else:
            modified_url = replace_hostname_in_url_randomly(url)
        return modified_url, use_https

    def _prepare_headers(self, accept_json: bool = True, content_type_json: bool = True, encoding: str | None = None) -> dict[str, Any]:
        """Prepare headers based on authentication and JSON settings."""
        headers: dict[str, Any] = {}
        if accept_json:
            headers["Accept"] = "application/json"
        if content_type_json:
            headers["Content-Type"] = "application/json"

        if encoding:
            headers["Content-Encoding"] = encoding

        if self.oidc:
            headers["Authorization"] = f"Bearer {self.id_token}"
            headers["Origin"] = self.auth_vo

        return headers

    def _prepare_ssl(self, use_https: bool) -> tuple[tuple[str, str] | None, bool | str]:
        """Prepare SSL configuration based on HTTPS usage and verification settings."""
        cert = None
        verify: bool | str = True
        if use_https:
            # the key is tested as well as the cert: requests wants both halves, and
            # __init__ sets them from the same _x509() call so they are set together
            if not self.oidc and self.ssl_certificate and self.ssl_key and os.path.isfile(self.ssl_certificate):
                cert = (self.ssl_certificate, self.ssl_key)

            if not self.verifyHost:
                verify = False
            elif "X509_CERT_DIR" in os.environ:
                verify = os.environ["X509_CERT_DIR"]
            elif os.path.exists("/etc/grid-security/certificates"):
                verify = "/etc/grid-security/certificates"
        return cert, verify

    # (0, the decoded response) or (EC_Failed, the error text)
    def get(self, url: str, data: dict[str, Any]) -> tuple[int, Any]:
        url, use_https = self._prepare_url(url)
        headers = self._prepare_headers()
        cert, verify = self._prepare_ssl(use_https)

        try:
            response = requests.get(url, headers=headers, params=data, timeout=600, cert=cert, verify=verify)
            response.raise_for_status()
            return 0, response.json()
        except requests.RequestException as e:
            return 255, str(e)

    def post(self, url: str, data: dict[str, Any]) -> tuple[int, Any]:
        url, use_https = self._prepare_url(url)
        headers = self._prepare_headers()
        cert, verify = self._prepare_ssl(use_https)

        try:
            response = requests.post(url, headers=headers, json=data, timeout=600, cert=cert, verify=verify)
            response.raise_for_status()
            return 0, response.json()
        except requests.RequestException as e:
            return 255, str(e)

    def post_files(self, url: str, data: dict[str, Any], encoding: str | None = None) -> tuple[int, Any]:
        url, use_https = self._prepare_url(url)
        headers = self._prepare_headers(content_type_json=False, encoding=encoding)
        cert, verify = self._prepare_ssl(use_https)

        files: dict[str, Any] = {}
        try:
            for key, value in data.items():
                if isinstance(data[key], str):
                    # we got a file to upload without specifying the destination name
                    files[key] = open(data[key], "rb")
                else:
                    # we got a file to upload which specifies the destination name
                    files[key] = (data[key][0], open(data[key][1], "rb"))
            print(f"cert: {cert}, verify: {verify}")
            response = requests.post(url, headers=headers, files=files, timeout=600, cert=cert, verify=verify)
            response.raise_for_status()
            return 0, response.json()
        except requests.RequestException as e:
            return 255, str(e)
        finally:
            for file in files.values():
                if isinstance(file, tuple):
                    file_handler = file[1]
                else:
                    file_handler = file
                file_handler.close()

    def override_oidc(self, oidc: bool, id_token: str | None, auth_vo: str | None) -> None:
        """
        Override OIDC settings.
        """
        self.oidc = oidc
        self.id_token = id_token
        self.auth_vo = auth_vo
