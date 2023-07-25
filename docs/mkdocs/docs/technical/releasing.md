# Release Tooling

This document details the release process for ArcticDB. 
ArcticDB is released onto [PyPi](https://pypi.org/project/arcticdb/) and [conda-forge](https://anaconda.org/conda-forge/arcticdb).

## 1. Create a new tag

Navigate to the [Tag Release](https://github.com/man-group/ArcticDB/actions/workflows/tag.yml) Github Action.

Click `Run Workflow` on the right hand side:
1. Type in the new version number
2. Click `Run workflow`.

Leave `Bump branch to the next version` as `No`.
This will create a branch off of `master` incrementing the version in `setup.cfg` but,
as of the time of writing, we are leaving that unchanged.

The [build will now be running for the tag.](https://github.com/man-group/ArcticDB/actions/workflows/build.yml)

## 2. Update conda-forge recipe

[`regro-cf-autotick-bot`](https://github.com/regro-cf-autotick-bot) generally opens a PR
on [ArcticDB's feedstock](https://github.com/conda-forge/arcticdb-feedstock)
for each new release of ArcticDB upstream.

You can update such a PR or create a new one to release a version, updating the
conda recipe. [Here's an example.](https://github.com/conda-forge/arcticdb-feedstock/pull/10)

You will need to update:

1. `version`, pointing to the tag created in step 1
2. The `sha256sum` of the source tarball
3. The build number (i.e. `number` under the `build` section) to `0`
4. Dependencies (if they have changed since the last version, see `setup.cfg`)
5. Rerender the feedstock's recipe to create Azure CI jobs' specification for all variants of the package

A PR is generally open with a todo-list summarizing all the required steps to perform,
before an update to the feedstock.

## 3. Release to PyPi

After building, GitHub Actions job you kicked off in step 2 after comitting
the tag will be waiting on approval to deploy to PyPi.
Find the job and click approve to deploy.

## 4. Release to Conda

Merge the PR created in step 3.

It will build packages, [pushing them to the `cf-staging` channel before publishing them
on the `conda-forge` channel for validation](https://conda-forge.org/docs/maintainer/infrastructure.html#output-validation-and-feedstock-tokens).

Packages are generally available a few dozen minutes after the CI runs' completion
on `main`.

## 5. Update the BSL conversion table

If you are releasing either a major (e.g. 2.0.0) or a minor (e.g. 1.5.0) release, please update the license conversion
schedule table [in the readme](https://github.com/man-group/ArcticDB/blob/master/README.md).

The conversion date will be two years from when the release is [published on GitHub](https://github.com/man-group/ArcticDB/releases/). This is not required if you are releasing a patch release.
