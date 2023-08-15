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

```


### mkdocs

To build mkdocs (to `/tmp/docs_build/` as an example):

```
mkdir /tmp/docs_build/
```

#### Working in Man Group:

To build:
```
docker run --rm -v /tmp/docs_build:/tmp/docs_build -v $(pwd)/mkdocs:/docs external-sandbox-docker.repo.prod.m/squidfunk/mkdocs-material:latest build -f mkdocs.yml -d /tmp/docs_build
```

To run the web server on port 8000 (from the `docs` directory):
```
docker run --rm -p 8000:8000 -v ${PWD}/mkdocs:/docs external-sandbox-docker.repo.prod.m/squidfunk/mkdocs-material:latest serve -f mkdocs.yml -a 0.0.0.0:8000
```

#### Working externally:

```
docker run --rm -v /tmp/docs_build:/tmp/docs_build -v $(pwd)/mkdocs:/docs squidfunk/mkdocs-material:latest build -f mkdocs.yml -d /tmp/docs_build
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

