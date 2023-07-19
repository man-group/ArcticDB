## Building

Work in this directory.

### mkdocs

To build mkdocs (to `/tmp/docs_build/` as an example):

```
mkdir /tmp/docs_build/
```

#### Working in Man Group:

* CentOS headnode
```
sudo docker run --rm --user $(id -u):$(id -g) -v /tmp/docs_build:/tmp/docs_build -v $(pwd)/mkdocs:/docs external-sandbox-docker.repo.prod.m/squidfunk/mkdocs-material:latest build -f mkdocs.yml -d /tmp/docs_build
```

* Ubuntu headnode
```
docker run --rm -v /tmp/docs_build:/tmp/docs_build -v $(pwd)/mkdocs:/docs external-sandbox-docker.repo.prod.m/squidfunk/mkdocs-material:latest build -f mkdocs.yml -d /tmp/docs_build
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

