import urllib.request, json
url = "https://hub.docker.com/v2/repositories/bitnami/spark/tags/?page_size=20"
req = urllib.request.Request(url)
response = urllib.request.urlopen(req)
data = json.loads(response.read())
for t in data['results']:
    print(t['name'])
