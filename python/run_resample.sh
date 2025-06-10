#!/bin/sh

for i in $(seq 1 1000); do
	python -m pytest -s tests/hypothesis/arcticdb/test_resample.py::test_resample_dynamic_schema || exit 1
done
