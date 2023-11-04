# docs-pages branch

This branch contains the built docs site.

## Download old versions of docs site and incorporate into this branch

Login to cloudflare dashboard and find the deployment for the version you want to download.

Use the account-id and deployment-id from the URL to get the json details for the deployment from this url:
`https://dash.cloudflare.com/api/v4/accounts/{account_id}/pages/projects/arcticdb-docs/deployments/{deployment_id}`

Save the file into the dir for the version you want, e.g. `4.0.2/orig_deployment.json`.
Run this code to download all the files using the json info.

```python
import requests
import json
import os

data = json.load(open('orig_deployment.json'))
url=data['result']['url']
files=data['result']['files'].keys()

for file in files:
    response = requests.get(url+file)
    if response.ok:
        filename = '.'+file
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'wb') as fobj:
            fobj.write(response.content)
    else:
        print(f"Error downloading {f}")
        exit(1)
```

Commit the result.

## Log

Version | Original deployment
------- | -------------------
1.1.0   | https://43b141e0.arcticdb-docs.pages.dev/
1.2.0   | https://1d0c2f26.arcticdb-docs.pages.dev/
1.2.1   | https://bd9d1e65.arcticdb-docs.pages.dev/
1.3.0   | https://f8d448e3.arcticdb-docs.pages.dev/
1.4.0   | https://9493c6ac.arcticdb-docs.pages.dev/
1.4.1   | https://95ca4b0f.arcticdb-docs.pages.dev/
1.5.0   | https://4c9f7a4c.arcticdb-docs.pages.dev/
1.6.0   | https://0a24934a.arcticdb-docs.pages.dev/
1.6.1   | https://daa72b63.arcticdb-docs.pages.dev/
1.6.2   | https://ae0878e7.arcticdb-docs.pages.dev/
2.0.0   | https://22c9973b.arcticdb-docs.pages.dev/
3.0.0   | https://5fb05a93.arcticdb-docs.pages.dev/
4.0.0   | https://77d84214.arcticdb-docs.pages.dev/
4.0.1   | https://84a2fd25.arcticdb-docs.pages.dev/
4.0.2   | https://2dd5f5ed.arcticdb-docs.pages.dev/
4.1.0   | New docs build action
Later   | Please use docs build action
