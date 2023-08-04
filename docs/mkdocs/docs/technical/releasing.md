# Release Tooling

This document details the release process for ArcticDB. 
ArcticDB is released onto [PyPi](https://pypi.org/project/arcticdb/) and [conda-forge](https://anaconda.org/conda-forge/arcticdb).

## 0. Check Internal Tests

Check Man Group's internal tests against the release candidate. See "ArcticDB - Support Process" internal wiki page.

## 1. Create a new tag

Navigate to the [Tag and Release](https://github.com/man-group/ArcticDB/actions/workflows/tag.yml) Github Action.

Click `Run Workflow` on the right hand side.

> [!IMPORTANT]  
> **Release candidate, full release, or hotfix?**
>
> We release a new version of ArcticDB on a [regular schedule](https://github.com/man-group/ArcticDB/milestones?direction=asc&sort=due_date&state=open). 
> This may involve promoting a release candidate to a full release as well as releasing a new release candidate.
>
> If preparing a release for a new _major_ or _minor_ version ([SemVer versioning](http://semver.org)), create a pre-release version (X.Y.ZrcN).
>
> If preparing a release for a new _patch_ version, create a full release (X.Y.Z).
>
> If there is an outstanding release candidate and no issues have been found, promote the release candidate to a full release (X.Y.Z).
>
> We will create a new release candidate for an existing pre-release version only to correct an issue that has been found with the additional functionality included in the prior pre-release version. In other words, we will promote rc1 to rc2 only if a bug has been found in functionality added in rc1, and not to provide a bugfix to functionality present prior to rc1.  

If creating a new release off master (pre-release or release version):
1. Type in the new version number eg `1.6.0` or `1.6.0rc0`.
2. Click `Run workflow`.

If promoting a pre-release:
1. Type in the new version number eg `1.6.0rc1` or `1.6.0`
2. Select the workflow off the source tag (e.g. `1.6.0rc0`)
3. Click `Run workflow`

If hotfixing an existing release then branch off the previously-released tag, apply the necessary changes (cherry-picking from master if commits are on master), and:
1. Type in the new version number eg `1.6.1`
2. Select the workflow off the source tag (e.g. `1.6.0`)
2. Click `Run workflow`.

This will create a branch off of `master`, incrementing the version specified in code. 

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

> [!IMPORTANT]  
> If releasing a pre-release version, you **must** merge the created PR into the `pr` branch and not the `main` branch. 
> This will require modifying the base branch of the created PR.

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
