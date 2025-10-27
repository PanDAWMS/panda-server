from pandaserver.api.v1.http_client import HttpClient as HttpClientV1
from pandaserver.api.v1.http_client import api_url_ssl as api_url_ssl_v1

http_client = HttpClientV1()

url = f"{api_url_ssl_v1}/pilot/update_worker_status"
data = {"worker_id": 9139456, "harvester_id": "CERN_central_k8s", "status": "started"}
status, output = http_client.post(url, data)
print(f"Status: {status}. Output: {output}")
