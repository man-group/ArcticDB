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

## 2. Update the version number in the tag

**This should not be a manual process - but is required for now!**

We need to update the public version identifier:
 - [CMake's `PROJECT_VERSION`](https://github.com/man-group/ArcticDB/blob/master/cpp/CMakeLists.txt#L5)
 - [`arcticdb.__version__`](https://github.com/man-group/ArcticDB/blob/master/python/arcticdb/\_\_init\_\_.py#LL14C1-L14C1).
 - `version` in the `metadata` section of [`setup.cfg`](https://github.com/man-group/ArcticDB/blob/master/setup.cfg)
 - the license conversion table in [the License section on `README.md`](https://github.com/man-group/ArcticDB/blob/master/README.md#license)

To do this, force push a new commit to the tag:

```
git fetch --all --tags
git checkout <NEWLY CREATED TAG>

# Edit files, e.g.
vi python/arcticdb/__init__.py

git add python/arcticdb/__init__.py
git commit -m 'Manually update version'
git tag <NEWLY CREATED TAG> -f
git push origin <NEWLY CREATED TAG> -f
```

The result should look similar to [this commit](https://github.com/man-group/ArcticDB/commit/c90a21a611b5c6ec2ef4b049981ac5c2ccb8ad08).

The [build will now be running for the tag.](https://github.com/man-group/ArcticDB/actions/workflows/build.yml)

## 3. Update conda-forge recipe

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

## 4. Release to PyPi

After building, GitHub Actions job you kicked off in step 2 after comitting
the tag will be waiting on approval to deploy to PyPi.
Find the job and click approve to deploy.

## 5. Release to Conda

Merge the PR created in step 3.

It will build packages, [pushing them to the `cf-staging` channel before publishing them
on the `conda-forge` channel for validation](https://conda-forge.org/docs/maintainer/infrastructure.html#output-validation-and-feedstock-tokens).

Packages are generally available a few dozen minutes after the CI runs' completion
on `main`.

## 6. After the release

You need to update the public version identifier so that it indicates that it is the development version for the next version (see [PEP 440](https://peps.python.org/pep-0440/#public-version-identifiers)).

For instance, if you have released `1.3.0` and the next version is planned to be `1.3.1`:
 - the public version identifier for Python has to be updated to `1.3.1.dev0` for:
   - [`arcticdb.__version__`](https://github.com/man-group/ArcticDB/blob/master/python/arcticdb/\_\_init\_\_.py#LL14C1-L14C1).
   - `version` in the `metadata` section of [`setup.cfg`](https://github.com/man-group/ArcticDB/blob/master/setup.cfg).
 - [CMake's `PROJECT_VERSION`](https://github.com/man-group/ArcticDB/blob/master/cpp/CMakeLists.txt#L5) has to be updated to `1.3.1` (since CMake does not (yet) support PEP 440)
