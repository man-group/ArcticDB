# Dynamic library permissions with ArticDB on AWS S3

## Goal

One of the advantages of ArcticDB is how easy it is to setup and use as a personal database.  But how can we extend this pattern to an organisation?  How can we keep it trivial to use as an individual, but allow for secure sharing of data with team-mates and groups across your organisation?  We also want this to be easy to maintain, a key challange for permissions generally.

Here we model a small two team organisation, 'Acme', in AWS and create some flexible permissions that allow users and teams to create
and use private and shared data without any per-library setup.

- Data Team
    - Jane
    - Samir
- Quant Team
    - Alan
    - Diana

Each user should be able to, 

- list all ArcticDB libraries (but not their content)
- create personal libraries that only they can read and write to
- create team libraries that only those in their team can read and write to

Users will follow an ArcticDB library name convention.  The library name should one of `<USERNAME>/<LIBRARY>` or `<TEAM>/<MYLIBRARY>`, so if Jane wants to create a personal library for weather data, they would use `lib.create_library('jane@acme/weather')` (assuming the AWS S3 setup below).

We can do this with path-prefix permissions in AWS S3.  Other backends, such as Minio, support path based permissioning.

## AWS S3

You should have a number of users setup in AWS IAM already along with a group containing all the users.  Follow the [IAM docs](https://docs.aws.amazon.com/IAM/latest/UserGuide/getting-started.html) for help with that.

For Acme we've setup four users and the users are tagged with the teams they are a member of:

| aws:username | aws:PrincipalTag/team |
| ------------ | --------------------- |
| jane@acme    | data                  |
| samir@acme   | data                  |
| alan@acme    | quant                 |
| diana@acme   | quant                 |

We've also created a user group, `acme` with all four users in.

Let's create an S3 bucket for Acme, `acme-arcticdb`, using [cloudshell](https://console.aws.amazon.com/cloudshell/).
```sh
aws s3 mb s3://acme-arcticdb
```

### Setting up the permissions policy

In general for read access our users will need `s3:ListBucket` and `s3:GetObject` permissions and for write access our users will additionally need `s3:PutObject` and `s3:DeleteObject`.

Then setup the following access policy.  We will save this snippet to `policy.json`.
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListObjects",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::acme-arcticdb",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "_arctic_cfg/*",
                        "${aws:username}/*",
                        "${aws:PrincipalTag/team}/*"
                    ]
                }
            }
        },
        {
            "Sid": "PutGetDeleteObjects",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::acme-arcticdb/${aws:username}/*",
                "arn:aws:s3:::acme-arcticdb/${aws:PrincipalTag/team}/*",
                "arn:aws:s3:::acme-arcticdb/_arctic_cfg/cref/?sUt?${aws:username}/*",
                "arn:aws:s3:::acme-arcticdb/_arctic_cfg/cref/?sUt?${aws:PrincipalTag/team}/*"
            ]
        }
    ]
}
```

Create the policy in AWS.
```sh
aws iam create-policy --policy-name acme-arcticdb-access --policy-document file://policy.json
```

Take note of the `Arn` in the output to the last command as you'll need it to attach the policy to a group
with all your users in, for this example the group is `acme`.
```sh
aws iam attach-group-policy --policy-arn <ARN> --group-name acme
```

If you intend to adapt that example policy to your own situation then please note that,
    - `acme-arcticdb` is the name of the bucket and will need to be replaced everywhere
    - `s3:ListBucket` is used to permission `ListObjectsV2` and needs its own section, as it applies to the bucket as a whole.  We control access to paths by checking the `s3:prefix` argument that's part of the `ListObjectsV2` request.
    - `Put`, `Get` and `Delete` can be specifed for object paths in the second section.
    - `_arctic_cfg/cref/*` is where the ArcticDB library configuration is stored and the data for each library is stored in the root of the bucket with a path that starts with the library name.
    - By using `${aws:username}` and `${aws:PrincipalTag/team}` we've restricted library access to those with a matching AWS IAM username or a matched user 'team' tag.

#### Security note

Because `${aws:username}`, `${aws:PrincipalTag/team}` and `_arctic_cfg`... are at the beginning of the path, it's important they don't contain values that can overlap. For example if you have a team called 'data' and a username for an application called 'data', they will have the same permissions, or if a username can be created that starts with `_arctic_cfg`... then that user will be able to modify all library configs.

## Usage

Jane can now list and read and write to their own libraries and team libraries, but not to others in Acme.

```python

import numpy as np
import arcticdb as adb

# jane@acme team=data
access = '<REDACTED>'
secret = '<REDACTED>'
bucket='acme-arcticdb'
region='eu-west-2'

arctic = adb.Arctic(f's3://s3.{region}.amazonaws.com:{bucket}?access={access}&secret={secret}')

# Create library as me
arctic.create_library('jane@acme/weather')
lib = arctic.get_library('jane@acme/weather')
lib.write('test', np.arange(100))

# Create library as data team
arctic.create_library('data/forecast')
lib = arctic.get_library('data/forecast')
lib.write('test', np.arange(100))

# See all libraries
arctic.list_libraries()
# ['alan@acme/bonds', 'data/forecast', 'jane@acme/weather', 'quant/stocks']

# Can't use or delete Alan or Quant team data
arctic.get_library('alan@acme/bonds')
arctic.get_library('quant/stocks')
arctic.delete_libraru('alan@acme/bonds')
# All raise:
# PermissionException: E_PERMISSION Permission error: S3Error#15 : No response body.
```