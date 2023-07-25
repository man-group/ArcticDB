## Building

Work in this directory.

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

## Uploading

Add the Sphinxdocs to the build:

```
mkdir /tmp/docs_build/api
cp -r ./sphinxdocs/build/html/* /tmp/mkdocs_build/api
```

Then `/tmp/docs_build/` forms the docs site.

