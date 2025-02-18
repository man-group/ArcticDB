# Migrating from Arctic

## ArcticDB vs Arctic

ArcticDB is API-compatible with [Arctic](https://github.com/man-group/arctic). Please note however that it **is not data compatible**. You cannot use ArcticDB to read Arctic data, nor use Arctic to write data that ArcticDB can read.

## How can I migrate from Arctic to ArcticDB

There are two ways that you can migrate data from Arctic to ArcticDB.

* **Manual migration**: The easiest way to migrate relatively small datasets is to read the data out of Arctic, and then write it using ArcticDB. As the ArcticDB is API compatible, this should be relatively simple.
* **Data Conversion**: Large data estates can be converted from Arctic to ArcticDB by using automated tooling that we can provide. For more information, please contact our commercial team via [our website](http://arcticdb.io) or email us at info@arcticdb.io. 
