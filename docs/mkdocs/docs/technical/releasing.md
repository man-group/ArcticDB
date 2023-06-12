# Release Tooling

This document details the release process for ArcticDB. ArcticDB is released onto [PyPi](https://pypi.org/project/arcticdb/) and [Conda](https://anaconda.org/conda-forge/arcticdb).

## 1. Create a new tag

Navigate to the [Tag Release](https://github.com/man-group/ArcticDB/actions/workflows/tag.yml) Github Action.

Click `Run Workflow` on the right hand side:
1. Type in the new version number
2. Click `Run workflow`.

Leave `Bump branch to the next version` as `No`. This will create a branch off of `master` incrementing the version in `setup.cfg` but, as of the time of writing, we are leaving that unchanged.

## 2. Update the version number in the tag

**This should not be a manual process - but is required for now!**

We need to update the version for [`arcticdb.__version__`](https://github.com/man-group/ArcticDB/blob/master/python/arcticdb/\_\_init\_\_.py#LL14C1-L14C1).

To do this, force push a new commit to the tag:

```
git fetch --all --tags
git checkout <NEWLY CREATED TAG>
vi python/arcticdb/__init__.py
git add python/arcticdb/__init__.py
git commit -m 'Manually update version'
git tag <NEWLY CREATED TAG> -f
git push origin <NEWLY CREATED TAG> -f
```

The result should look similar to [this commit](https://github.com/man-group/ArcticDB/commit/c90a21a611b5c6ec2ef4b049981ac5c2ccb8ad08).

The [build will now be running for the tag.](https://github.com/man-group/ArcticDB/actions/workflows/build.yml)

## 3. Update conda-forge recipe

Create a new PR to update the Conda recipe. [Here's an example.](https://github.com/conda-forge/arcticdb-feedstock/pull/10)

You will need to update:

1. The version, pointing to the tag created in step 1. 
2. The sha of the source tarball
3. Dependencies (if they need to be changed)

Don't forget to re-render!

## 4. Release to PyPi

After building, GitHub Actions job you kicked off in step 2 after comitting the tag will be waiting on approval to deploy to PyPi. Find the job and click approve to deploy.

## 5. Release to Conda

Merge the PR created in step 3. 

It will build and push to conda-forge.