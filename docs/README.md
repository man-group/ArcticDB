## Building

Work in this directory, except for the `doxygen` documentation.
The `doxygen` documentation needs to be build from `docs/doxygen`

### doxygen

The doxygen documentation is built from the `docs/doxygen` directory.
Doxygen does not need articdb installed, or built, to build the documentation.
The only requirement are `doxygen` itself and `graphviz` for the diagrams.
Both can be installed via `conda` / `mamba` or other package managers.
To build the documentation, run `doxygen` from the `docs/doxygen` directory.
This will create a `docs/doxygen/docs/html`  directory with the documentation.
Open `docs/doxygen/docs/html/index.html` in the browser of your choice to just view the doxygen docs.
Note that we currently set the `Doxyfile` in such a way that **everything** is documented
and diagrams are generated, even undocumented classes / functions. This gives about 400 MB of documentation (including diagrams).

### mkdocs

Dependencies

- mkdocs-material: theme
- mkdocs-jupyter: jupyter notebook support
- mkdocstrings[python]: python docstring support, like sphinx docs
- black: for signature formatting
- pybind11-stubgen: for generating stubs for pybind11 extensions, so that mkdocstrings can parse them
- mike: for deploying versioned docs

Install
```
pip3 install arcticdb[docs]
```
or
```
mamba install mkdocs-material mkdocs-jupyter mkdocstrings-python black pybind11-stubgen mike wheel
```

You need to have ArcticDB installed to generate the API docs, so install it from source:

```
cd $PROJECT_ROOT
python setup.py bdist_wheel
pip install <generated wheel>
```

or from PyPi,
```
pip install arcticdb
```

`mkdocstrings[python]` doesn't support python extensions very well, so create interface files (pyi) for the extensions first.
```
cd docs/mkdocs
# TODO fix these errors!
pybind11-stubgen arcticdb_ext --ignore-all-errors -o .
```

To build the latest mkdocs to docs/mkdocs/site:
```
cd docs/mkdocs
mkdocs build
```

Development server 
```
mkdocs serve -a 0.0.0.0:8000
```

The python docstring parser, griffe, will struggle with some built-in (C++) modules
You'll see 'alias' errors as this when it can't resolve something:
```
ERROR   -  mkdocstrings: Template 'alias.html' not found for 'python' handler and theme 'material'.
ERROR   -  Error reading page 'api/library.md': alias.html
```

We use `mike` to version the docs.  The docs are first built into the `docs-pages` branch and then deployed to `docs.arcticdb.io` from there.

To serve the versioned docs locally, run `mike serve` from the `docs/mkdocs` directory.

## Building Docs using the Github Action

To use the `Docs Build` github action follow these steps:

* Click the `run workflow` dropdown
* Select a git branch for the code to run the action (normally `master`)
* The version text for the action should be in the form `4.0.2` (as an example) or empty for a `dev` build
  * In case of a versioned build (e.g. `4.0.2`), the action will use git tag `v4.0.2-docs` to build the docs
  * In case of a `dev` build, the docs are built from the selected branch HEAD
* Tick the `latest` box if are working on docs for the latest stable release
* Tick the push to github box unless you are testing the docs build. Normally you would tick this, because the publish action reads from there.

For managing git, please follow these conventions:

* To make the first mod to the docs for an older version please branch from the `v4.0.2` tag with a branch called `v4.0.2-docs-branch`
* If the branch and docs tag already exist for the version the just checkout the branch
* Commit the changes you want to see in the docs for that version
* If it exists already, delete the `v4.0.2-docs` tag using 
    * `git tag -d v4.0.2-docs` (local)
    * `git push origin :v4.0.2-docs` (remote)
* Recreate the tag for your latest commit using
    * `git tag v4.0.2-docs`
* Push your commit and new tag using
    * `git push` (changes)
    * `git push origin v4.0.2-docs` (tag)
* Run the action

The reason for the separate docs tags is to allow improvements to the docs to be made to recent versions without re-tagging the release. The docs branches can eventually be deleted for older versions. The tags should not be deleted.


## Deployment of `docs.arcticdb.io`

[docs.arcticdb.io](https://docs.arcticdb.io) is hosted by netlify and runs continous deployment on the `docs-pages` branch of ArcticDB.
