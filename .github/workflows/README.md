CI system guide
===============

<!--
<tr><th></th><td></td>
-->

# Introduction

This is the documentation of the CI system that is used to build and publish ArcticDB.
It aims to provide an overview of the system and the general steps.

The CI system is based on GitHub Actions.
It can be triggered automatically on push to any branch or manually [here](https://github.com/man-group/ArcticDB/actions/workflows/build.yml).
There is also a scheduled build of the master branch that runs every night and tests against real, persistent storages (e.g. AWS S3, Azure Cloud, etc).

# Overview

``` mermaid
---
Build Steps
---
flowchart LR
    A((trigger)) --> common_config
    common_config --> cibw_docker_image
    common_config --> windows_compile
    cibw_docker_image --> linux_compile
    subgraph leader
        direction LR
        windows_compile
        linux_compile
    end
    subgraph follower
        direction LR
        windows_python_tests
        linux_python_tests
    end
    subgraph cpp_tests
        direction LR
        cpp_tests_windows_compile --> cpp_tests_windows
        cpp_tests_linux_compile --> cpp_tests_linux
    end
    subgraph NOT YET IMPLEMENTED
        subgraph persistent_storage_tests
            direction LR
            pre_persistent_storages_test --> persistent_storages_test
            persistent_storages_test --> post_persistent_storages_test
        end
    end
    leader --> follower
    leader --> cpp_tests
    leader --> persistent_storage{Should test against real storages?} --> persistent_storage_tests --> can_merge
    follower --> docs
    docs --> can_merge
    cpp_tests --> can_merge
    can_merge --> pub_check{publish_env} --> publish
```

This diagram shows the structure of the CI system.
The system is defined in the [build.yml](build.yml) file.
The concrete steps are implemented in the [build_steps.yml](build_steps.yml) and [persistent_storage.yml](persistent_storage.yml) files.
For more information, see the [Description of the YML files](#description-of-the-yml-files) section.

## Common config job

This job is used to configure the CI run.
It sets up the subsequent jobs and where they will be executed.
This is where environment variables are set and also some of the variables that are used by the later jobs.
You can see them in the outputs part and they are used by the other jobs by calling *needs.common_config.outputs...*.

## CI Build Wheel Docker Image

TODO

## Leader jobs

The leader jobs are designed to do the C++ core compilation using one Python version to seed the compilation caches.
This way, if there is a general problem with the build, it can fail quicker, without having to wait for the different versions.
There are leader jobs for both Linux and Windows.

## C++ Tests jobs

These jobs compile and run the C++ tests.
They are designed to run concurrently with the Follower jobs, which test against the different versions of Python.

## Follower jobs

After the leader jobs have passed successfully, we run Follower jobs that compile and test against the all of the supported Python versions.

## Docs job

The Docs job compiles and publishes the latest docs.
It needs a valid ArcticDB wheel to run, so it depends on the Linux jobs.
Also currently, the automatic builds trigger only from changes in the code.
**So if you make changes that are only in the docs, you will need to start a manual build and supply a valid ArcticDB wheel.**

## can_merge check

This is a simple check that is used by GitHub to determine, if a PR can be merged.
It is also used as a gate to check, if we can publish a new release.

## Publish

In this step, we prepare the release notes and publish to PyPi.
This step cannot run on automatic builds (except for [version tag builds](https://github.com/man-group/ArcticDB/blob/master/.github/workflows/build.yml#L59)), because it requires the *pypi_publish* argument to be enabled, which can be done only with a manual build.

# Build Inputs

When a manual build is triggered, the user can specify the following input arguments:

- Use workflow from - this can be used to select the branch that needs to be built
- pypi_publish - a boolean option on whether or not the build should publish to PyPi
- Environment to publish to - to select where to publish to (e.g. ProdPypi or Test Pypi)
    - used for publishing both the docs and the wheels
    - has no effect, unless pypi_publish=true
- Override CMAKE preset type - to override the build type (e.g. release vs debug)
- persistent_storage - whether the built should execute tests that rely on real storages (e.g. AWS S3)

# Description of the YML files

## [build.yml](build.yml)

* Runs the main CI pipeline for the project
* Sets up the main triggers for the build:
    * on push
    * manual
* Sets up the general structure of the build

The logic for the steps is implemented in [build_steps.yml](build_steps.yml).

### Settings
<table>
<tr><th>inputs.pypi_publish</th><td>Specifies if the build should publish a new PyPi release</td>
<tr><th>inputs.publish_env</th><td>Environment to publish to, if <code>inputs.pypi_publish</code> is enabled</td>
<tr><th>inputs.cmake_preset_type</th><td>Override CMAKE preset type with release or debug</td>
<tr><th>inputs.persistent_storage</th><td>Specifies if the build should run the tests that use real storage (e.g. AWS S3)</td>
</table>

## [build-steps.yml](build-steps.yml)

This is where the actual logic for the build steps is implemented.
This logic is used in [build.yml](build.yml).

### Settings
<table>
<tr><th>inputs.job_type</th><td>Selects the steps to enable based on the job type (e.g. leader, follower, cpp-test, persistent_storage</td>
<tr><th>inputs.cmake_preset_type</th><td>release or debug</code> is enabled</td>
<tr><th>inputs.matrix</th><td>JSON string to feed into the build matrix</td>
<tr><th>inputs.cibw_image_tag</th><td>Linux only. As built by cibw_docker_image.yml workflow</td>
<tr><th>inputs.cibw_version</th><td>Follower only. Must match the cibw_image_tag</td>
<tr><th>inputs.python_deps_ids</th><td>Follower test matrix parameter. JSON string.</td>
<tr><th>inputs.python3</th><td>Specifies the Python 3 minor version (e.g. 6, 7, 8, etc.)</td>
</table>

## [tag.yml](tag.yml)

* Creates a git tag from the selected branch
* Optionally bump the selected branch to the next version

This can only be invoked manually by contributors and can require further approval using the `TestPypi`
environment's protection rules.

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

**Runs on forks**: Yes. Must create two environments named `TestPypi` and `ProdPypi` with Pypi credentials.

### Call patterns
* Called at the end of a `build.yml` build with `pypi_publish` explicit set or resolved to true (e.g. version tag build)
* Manual run (workflow dispatch) - Use Environment protection rules to prevent accidentally releasing the wrong branch

### Settings
See also: [`twine` docs](https://twine.readthedocs.io/en/stable/#environment-variables).

<table>
<tr><th>inputs.environment</th><td>Contains the deployment secrets. Should protect with branch rules and approvers</td>
<tr><th>inputs.run_id</th><td>For manual runs, specify the GitHub Action run ID to gather artifacts from</td>
<tr><th>vars.TWINE_USERNAME<br>or secrets.TWINE_USERNAME</th><td colspan="2">Please set API tokens, not real user names and passwords.</td>
<tr><th>secrets.TWINE_PASSWORD</th>
<tr><th>vars.TWINE_REPOSITORY</th><td>Well know repos like pypi OR TestPypi</code>
<tr><th>vars.TWINE_REPOSITORY_URL</th><td>Alternatively specify the URL of the repo</td>
<tr><th>vars.TWINE_CERT</th><td>SSL CA</td>
</table>

## [docs.yml](docs.yml)

**Runs on forks**: Yes. Must supply a CloudFlare Pages site to upload to

### Call patterns
| Called from       | Environment   | Intended effect
|-------------------|---------------|----------------
| master build      | TestPypi      | Updates the preview site
| version tag build | ProdPypi      | Updates the public/`main` site
| other build^      | null          | Doc syntax check only
| workflow_dispatch | user supplied | Per environment settings

^ `build.yml` is triggered by changes to the code directories only.
If you pushed only docs changes, please use the workflow dispatch to run a build manually
(and supply a suitable ArcticDB wheel from a previous build).

### Settings
<table>
<tr><th>inputs.environment</th><td>Contains the deployment secrets. Should protect with branch rules and approvers</td>
<tr><th>inputs.api_wheel</th><td>In manual runs, overrides the wheel used for Sphinx/API docs generation</td>
<tr><th>vars.CLOUDFLARE_ACCOUNT_ID</th><td colspan="2">See
        <a href="https://developers.cloudflare.com/workers/wrangler/system-environment-variables/">CF docs</a></td>
<tr><th>secrets.CLOUDFLARE_API_TOKEN</th>
<tr><th>vars.CLOUDFLARE_PAGES_PROJECT</th><td>Pages project name</td>
<tr><th>vars.CLOUDFLARE_PAGE_BRANCH</th><td>Even if our CF site is not directly deploying from a git repo,
    it still uses the concept of branches to distinguish "deployments."<br>
    The visibility of each branch is set via the CF console. <code>main</code> is typically the public site.<br>
    Hint: define this variable in Environments<br>
    If not set, will publish to GitHub Pages instead.</td>
</table>
