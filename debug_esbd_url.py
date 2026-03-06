import requests

URL = "https://www.txsmartbuy.gov/app/extensions/CPA/CPAMain/1.0.0/services/ESBD.Service.ss"
HEADERS = {
    "x-sc-touchpoint": "shopping",
    "x-requested-with": "XMLHttpRequest",
    "content-type": "application/json; charset=UTF-8",
    "referer": "https://www.txsmartbuy.gov/esbd",
    "user-agent": "Mozilla/5.0",
}
resp = requests.post(
    URL,
    params={"c": "852252", "n": "2"},
    headers=HEADERS,
    json={"lines": [], "page": 1, "urlRoot": "esbd"},
    timeout=20,
)
for item in resp.json().get("lines", [])[:15]:
    print(item.get("internalid"), "|", item.get("solicitationId"), "|", repr(item.get("url")))
