CI system guide
===============

<!--
<tr><th></th><td></td>
-->

## [tag.yml](tag.yml)

* Creates a git tag from the selected branch (where allowed by the `TestPypi` environment's protection rules)
* Optionally bump the selected branch to the next version

### Settings
<table>
<tr><th>inputs.version</th><td>Must be a valid <a href="https://python-semver.readthedocs.io/en/latest/">SemVer3</a>
    string</td>
<tr><th>inputs.overwrite</th><td>If true, the tag will be pushed with <code>-f</code></td>
<tr><th>inputs.bump</th><td>Select the part of <code>inputs.version</code> to increment.
    The new version will be written to the source branch without PR.</td>
<tr><th>secrets.TAGGING_TOKEN</th><td>GitHub doesn't (recursively) trigger Actions from tags pushed by an Action.<br>
    To make the tag build automatically, set this to a GitHub access token with content write access.<br>
    Note: GH will log the "Deployment" (the part of the tag build that uses Environment secrets) as "on behalf of"
    the token's creator.</td>
</table>

## [publish.yml](publish.yml)

* Gathers the wheels and uploads them to Pypi
* Generates a draft GitHub Release and attaches debug artifacts

**Runs on forks**: Yes. Must create two environments named `TestPypi` and `ProdPypi` with Pypi creds.

### Settings
See also: [`twine` docs](https://twine.readthedocs.io/en/stable/#environment-variables).

<table>
<tr><th>inputs.environment</th><td>Contains the deployment secrets. Should protect with branch rules and approvers</td>
<tr><th>inputs.run_id</th><td>For manual runs, specify the GitHub Action run ID to gather artifacts from</td>
<tr><th>vars.TWINE_USERNAME<br>or secrets.TWINE_USERNAME</th><td colspan="2">Please set API tokens, not real user names and passwords.</td>
<tr><th>secrets.TWINE_PASSWORD</th>
<tr><th>vars.TWINE_REPOSITORY</th><td>Well know repos like pypi OR testpypi</code>
<tr><th>vars.TWINE_REPOSITORY_URL</th><td>Alternatively specify the URL of the repo</td>
<tr><th>vars.TWINE_CERT</th><td>SSL CA</td>
</table>
