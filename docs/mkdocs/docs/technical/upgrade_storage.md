# Upgrade storage config

You only need to follow this guide when pointed to it by an error message when accessing your library or when asked to
during an upgrade.

This indicates that the stored config for your library is unsupported by this version of ArcticDB.

The rest of this guide explains how to update the stored config across all libraries in an Arctic instance.

Since this requires write access on the storage, this should be performed by a suitably permissioned user.

## Upgrade script

### Pre-Requisites

- Ensure that all users are on at least version 3.0.0 of ArcticDB
- Install latest ArcticDB `pip install -U arcticdb` or `conda install -c conda-forge arcticdb`
- You must have _write_ credentials on the storage you are using as your ArcticDB backend
(eg S3 bucket / Azure blob storage)
- Create a `uri` suitable to use with an `Arctic` instance for your backend, with write credentials. See [docs](https://docs.arcticdb.io/api/arcticdb#arcticdb.Arctic.__init__)
- Run `arcticdb_update_storage --uri "<uri>"` where `<uri>` is that created in the step above. This will not modify
anything, but will log the affected libraries with "Config NOT OK for <library_name>"

### Run Script

If no libraries were shown as affected after following the steps above, you can stop now. You do not need to do any more.

> :warning: Running this script will break access for clients on less than version 3.0.0 of ArcticDB for affected libraries.
> The affected libraries were shown in the step above.
> Ensure users have upgraded to at least version `arcticdb==3.0.0` first.

- Run `arcticdb_update_storage --uri "<uri>" --run` where `<uri>` is the same as the one above.


## Release History

| ArcticDB Version | Upgrade                                     | Github Issue |
|------------------|---------------------------------------------|--------------|
| 3.0.0            | Removes credentials from stored config.     | #802         |
