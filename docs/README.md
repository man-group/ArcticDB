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
pip install mkdocs-material mkdocs-jupyter mkdocstrings[python] black pybind11-stubgen
```
- mkdocs-material: theme
- mkdocs-jupyter: jupyter notebook support
- mkdocstrings[python]: python docstring support, like sphinx docs
- black: for signature formatting
- pybind11-stubgen: for generating stubs for pybind11 extensions, so that mkdocstrings can parse them


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
cd python
# TODO fix these errors!
pybind11-stubgen arcticdb_ext.version_store --ignore-all-errors -o .
```

To build mkdocs to docs/mkdocs/site:
```
cd docs/mkdocs
mkdocs build -s
```

Development server 
```
mkdocs serve -s -a 0.0.0.0:8000
```

The python docstring parser, griffe, will struggle with some built-in (C++) modules
You'll see 'alias' errors as this when it can't resolve something:
```
ERROR   -  mkdocstrings: Template 'alias.html' not found for 'python' handler and theme 'material'.
ERROR   -  Error reading page 'api/library.md': alias.html
```


## Deploying to `docs.arcticdb.io`

Run the `Docs` github action.
- Branch: Master
- Environment: ProdPypi
- Override: arcticdb

