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

Install
```
pip install mkdocs-material mkdocs-jupyter mkdocstrings[python] black
```
- mkdocs-material: theme
- mkdocs-jupyter: jupyter notebook support
- mkdocstrings[python]: python docstring support, like sphinx docs
- black: for signature formatting

To build mkdocs (to `/tmp/docs_build/` as an example):
```
cd docs/mkdocs
mkdocs build -d /tmp/docs_build
```

Development server 
```
mkdocs serve -s -v -a 0.0.0.0:8000
```

The python docstring parser, griffe, will struggle with some built-in (C++) modules
You'll see 'alias' errors as this:
```
ERROR   -  mkdocstrings: Template 'alias.html' not found for 'python' handler and theme 'material'.
ERROR   -  Error reading page 'api/library.md': alias.html
```
which is why we set this in mkdocs.yml
```
preload_modules:
    - arcticdb_ext
```


### Sphinx

You need to have the ArcticDB wheel installed, so install it from source:

```
cd $PROJECT_ROOT
python setup.py bdist_wheel
pip install <generated wheel>
```

or from PyPi,

```
pip install arcticdb
```

Install dependencies,

```
pip install sphinx sphinx_rtd_theme
```

Build,

```
cd sphinxdocs
make html
cd ..
```

This writes build files to `./sphinxdocs/build/html`.
Open `./sphinxdocs/build/html/index.html` in the browser of your choice to just view the Sphinx docs.

## Uploading

Add the Sphinxdocs to the build:

```
mkdir /tmp/docs_build/api
cp -r ./sphinxdocs/build/html/* /tmp/mkdocs_build/api
```

Then `/tmp/docs_build/` forms the docs site.

## Deploying to `docs.arcticdb.io`

Run the `Docs` github action.
- Branch: Master
- Environment: ProdPypi
- Override: arcticdb

This will generate both the mkdocs and sphinx pages.

