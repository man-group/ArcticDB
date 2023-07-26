# Release Tooling

This document details the release process for ArcticDB. 
ArcticDB is released onto [PyPi](https://pypi.org/project/arcticdb/) and [conda-forge](https://anaconda.org/conda-forge/arcticdb).

## 0. Check Internal Tests

Check Man Group's internal tests against the release candidate. See "ArcticDB - Support Process" internal wiki page.

## 1. Create a new tag

Navigate to the [Tag Release](https://github.com/man-group/ArcticDB/actions/workflows/tag.yml) Github Action.

Click `Run Workflow` on the right hand side:
1. Type in the new version number eg `1.6.0`.
2. Click `Run workflow`.

Leave `Bump branch to the next version` as `No`.
This will create a branch off of `master` incrementing the version in `setup.cfg` but we ignore it for now.

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

The GitHub Actions "Build and Test" job kicked off automatically at the end of step 1 will wait on approval to deploy to PyPi.

Find the job and click approve to deploy.

## 4. Release to Conda

Merge the PR created in step 3.

It will build packages, [pushing them to the `cf-staging` channel before publishing them
on the `conda-forge` channel for validation](https://conda-forge.org/docs/maintainer/infrastructure.html#output-validation-and-feedstock-tokens).

Packages are generally available a few dozen minutes after the CI runs' completion
on `main`.

## 5. Update the BSL conversion table

If you are releasing either a major (e.g. 2.0.0) or a minor (e.g. 1.5.0) version, please update the license conversion
schedule table [in the readme](https://github.com/man-group/ArcticDB/blob/master/README.md).

The conversion date will be two years from when the release is [published on GitHub](https://github.com/man-group/ArcticDB/releases/). This is not required if you are releasing a patch release.

# Removing a release

**Note** - instructions in this section are only to be followed if a broken build has been released that needs to be removed. 
This section can be ignored under normal circumstances.

## Conda-forge

Conda-forge packages are immutable. As a result, packages cannot be removed entirely.

[They can, however, be marked as `broken`.](https://conda-forge.org/docs/maintainer/updating_pkgs.html#removing-broken-packages) 
This adds a new label to the package, `broken`. 
Packages with the `broken` label must be installed via a special channel (`conda install -c conda-forge/label/broken`) and thus for most users won't be visible. 

To mark an ArcticDB package as broken:

1. [Find all existing files for the given version](https://anaconda.org/conda-forge/arcticdb/files?version=1.6.0rc0&channel=main)
2. Create a PR that adds a new file listing every file to be marked as broken. [Here's an example](https://github.com/conda-forge/admin-requests/pull/765).
3. Wait for someone from the Core team to merge the PR.
