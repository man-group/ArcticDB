#!/bin/bash

find . -type l -delete

for f in ../lmdb/libraries/liblmdb/{mdb.c,lmdb.h,midl.?}
do 
    ln -s $f
done

ln -s ../lmdbxx/lmdb++.h
