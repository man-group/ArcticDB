# docs-pages branch

[![Netlify Status](https://api.netlify.com/api/v1/badges/37112dbd-bb1e-4d2e-b90a-5f13572ffd42/deploy-status)](https://app.netlify.com/sites/arcticdb-docs/deploys)

This branch contains the built docs site.

## Serve locally

When on the docs-pages branch, run this command to serve the site locally:
```bash
python3 -m http.server
```

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

## Patch the version selector and warning into the pre-versioned docs

Run this script in the subdir for the documentation version.

```python
from os import walk, path

outdated = """
<div data-md-color-scheme="default" data-md-component="outdated" hidden>
    <aside class="md-banner md-banner--warning">
        <div class="md-banner__inner md-grid md-typeset">           
            You're not viewing the latest version.
            <a href="../.">
                <strong>Click here to go to latest.</strong>
            </a>
        </div>
        <script>var el=document.querySelector("[data-md-component=outdated]"),outdated=__md_get("__outdated",sessionStorage);!0===outdated&&el&&(el.hidden=!1)</script>
    </aside>
</div>
"""

config = ', "version": {"default": "latest", "provider": "mike"}'


for dirpath, _, fnames in walk("./"):
    for f in fnames:
        filename = path.join(dirpath, f)
        if filename.endswith(".html"):
            with open(filename, "r") as fobj:
                filedata = fobj.read()
            if outdated not in filedata:
                print(filename, " adding outdated warning")
                filedata = filedata.replace('<header class="md-header', outdated + '<header class="md-header')
            if config not in filedata:
                print(filename, " adding config")
                filedata = filedata.replace('"Select version"}', '"Select version"}' + config)
            with(open(filename, "w")) as fobj:
                fobj.write(filedata)
```

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
