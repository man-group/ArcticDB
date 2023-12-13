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
> If preparing a release for a new _major_ or _minor_ version ([SemVer versioning](http://semver.org)),
create a pre-release version (X.Y.ZrcN).
>
> If preparing a release for a new _patch_ version, create a full release (X.Y.Z).
>
> If there is an outstanding release candidate (look [here](https://pypi.org/project/arcticdb/#history))
and no issues have been found, promote the release candidate to a full release (X.Y.Z).
>
> Run `./release_checks.py` against a release candidate before promoting it to a full release.

If creating a new release off master:
1. Type in the new version number eg `1.6.0` or `1.6.0rc0`.
2. Click `Run workflow`.

If promoting a pre-release:
1. Type in the new version number eg `1.6.0rc1` or `1.6.0`
2. Select the workflow off the source tag (e.g. `1.6.0rc0`)
3. Click `Run workflow`

The [build will now be running for the tag.](https://github.com/man-group/ArcticDB/actions/workflows/build.yml)

## 2. Update conda-forge recipe

A new release on conda-forge is made by creating a Pull Request on the feedstock: [`conda-forge/arcticdb-feedstock`](https://github.com/conda-forge/arcticdb-feedstock).

For more information about conda-forge release process and feedstocks' maintenance,
see [this section of the documentation of conda-forge](https://conda-forge.org/docs/maintainer/updating_pkgs.html).

If you have created a tag for a release on ArcticDB's repository, [`regro-cf-autotick-bot`](https://github.com/regro-cf-autotick-bot)
might already have opened a Pull Request to create a new release.
You can push commits to this PR or alternatively create a new PR to release a version.

> [!IMPORTANT]
> **Do not commit directly to the feedstock repository.**
> Commits to the repository release new versions to conda-forge. Instead, changes **must** be made
via personal forks or via the PR created by the [`regro-cf-autotick-bot`](https://github.com/regro-cf-autotick-bot) as mentioned above.
>
> If publishing a normal release, you **must** create two pull-requests due to some users' constraints
> (see the description of the introduction of the `libevent-2.1.10` branch in
> [this PR](https://github.com/conda-forge/arcticdb-feedstock/pull/64) for more context):
>  - one pull request which branches from `main` and merge the created PR into the `main` branch.
>  - one pull request which branches from `libevent-2.1.10` and merge the created PR into the `libevent-2.1.10` branch.
>
> If publishing a release-candidate, you **must** branch from `rc` and merge the created PR into the `rc` branch.

Do the following for each PR:
1. [Fork `arcticdb-feedstock`](https://github.com/conda-forge/arcticdb-feedstock/fork)
2. Create a branch of the base branch for your case (see above)
3. Change [`recipe/meta.yaml`](https://github.com/conda-forge/arcticdb-feedstock/blob/main/recipe/meta.yaml)
to update the following pieces of information:
    - `version`, pointing to the tag created in step 1
    - `sha256sum` of the source tarball 
    - the build number (i.e. `number` under the `build` section) to `0`
    - the dependencies if they have changed since the last version in:
      - [`setup.cfg`](https://github.com/man-group/ArcticDB/blob/master/setup.cfg)
      - [`environment_unix.yml`](https://github.com/man-group/ArcticDB/blob/master/environment_unix.yml)
4. Push on your fork and create a PR to the feedstock targeting the base branch on `conda-forge/arcticdb`
5. Rerender the feedstock's recipe to create Azure CI jobs' specification for all variants of the package 
   This can be done by posting a comment on the PR with the following content `@conda-forge-admin, please rerender`

Example pull-requests:
 - for normal release:
     - [the PR used to publish `4.0.0` which targets `main`](https://github.com/conda-forge/arcticdb-feedstock/pull/67)
     - [the PR used to publish `4.0.0` which targets `libevent-2.1.10`](https://github.com/conda-forge/arcticdb-feedstock/pull/68)
 - for a release candidate: [the PR used to publish `3.0.0rc1` with targets `rc`](https://github.com/conda-forge/arcticdb-feedstock/pull/55)

## 3. Release to PyPi

The GitHub Actions "Build and Test" job kicked off automatically at the end of step 1 will wait on approval to deploy to PyPi.

Find the job and click approve to deploy.

## 4. Release to Conda

Merge the PR created in step 3.

It will build packages, [pushing them to the `cf-staging` channel before publishing them
on the `conda-forge` channel for validation](https://conda-forge.org/docs/maintainer/infrastructure.html#output-validation-and-feedstock-tokens).

Packages are generally available about half an hour after the CI completes on `main`.

You can search for your package to see if it has been published with:

```
mamba search 'conda-forge/label/arcticdb_rc::arcticdb==4.1.0rc0'  # RC
mamba search 'conda-forge::arcticdb==4.0.0'  # Normal
```

## 5. Update the BSL conversion table

If you are releasing either a major (e.g. 2.0.0) or a minor (e.g. 1.5.0) version, update the license conversion
schedule table [in the readme](https://github.com/man-group/ArcticDB/blob/master/README.md).

The conversion date will be two years from when the release is [published on GitHub](https://github.com/man-group/ArcticDB/releases/). This is not required if you are releasing a patch release.

## 6. Docs

To release the docs,
- Run the [Docs Build action](https://github.com/man-group/ArcticDB/actions/workflows/docs_build.yml) for your new tag with `deploy` selected.  If this is the latest stable release of ArcticDB (i.e. not a backport) then also select `latest`.
- Run the [Docs Publish action](https://github.com/man-group/ArcticDB/actions/workflows/docs_publish.yml) with the Prod environment.  The Publish action needs approval and will upload to https://docs.arcticdb.io (hosted by Cloudflare Pages).

See the [Docs README](https://github.com/man-group/ArcticDB/blob/master/docs/README.md) for more information.

## 7. Release Notes

Only do this for normal releases, not release candidates.

After the PyPi release, draft release notes will appear [here](https://github.com/man-group/ArcticDB/releases/).

Edit them and take particular care to announce API changes and describe important new features and
bugfixes. When satisfied, decide whether to tick "set as latest release", and press publish.

Then copy the notes and announce the release in Slack, in the ArcticDB #general channel and the
Man Group #arcticdb-announcements channel.

# Dealing with bad releases

**Note** - instructions in this section are only to be followed if a broken build has been released.
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

# Hotfix Process

If hotfixing an existing release (or pre-release) then branch off the previously-released tag, apply the necessary changes
(cherry-picking from master if commits are on master), and run the "Tag and Release" job.

1. Type in the new version number eg `1.6.1` (or `1.6.0rc2` as an example of a pre-release version)
2. Select the workflow off the source tag (e.g. `1.6.0` or `1.6.0rc1`)
3. Click `Run workflow`.

Then follow the steps in the main part of this document for 2 downwards as usual.

