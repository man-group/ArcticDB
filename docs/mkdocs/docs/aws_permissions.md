# Permissioning ArticDB in AWS S3

## Goal

One of the advantages of ArcticDB is how easy it is to setup and use as a personal database.  But how can we extend this pattern to an organisation?  How can we keep it trivial to use as an individual, but allow for secure sharing of data with team-mates and groups across your organisation?  We also want this to be easy to maintain, a key challange for permissioning systems.

Here we model a small two team organisation, 'Acme', in AWS and create some flexible permissions, that allow users and teams to create
and use private and shared data.  The organisation consists of two teams with two employees in each.

- Data Team
  - Jane
  - Matthew
- Quant Team
  - Alan
  - Diana

Each user should be able to, 
- list all ArcticDB libraries (but not their content)
- create personal libraries that only I can read and write to
- create team libraries that only those in my team can read and write to

We can do this with a combination with conventions in ArcticDB and path-prefix permissions in S3.  A number of S3 backends support path based permissioning.  Here we show how to do it with AWS IAM and S3 permissions.

## AWS S3

We'll assume you have a number of users and groups (teams) setup in AWS IAM already.  Follow the [IAM docs](https://docs.aws.amazon.com/IAM/latest/UserGuide/getting-started.html) for help with that.

For our examples we've setup four users and the users are tagged with the teams they are a member of:

| aws:username | aws:PrincipalTag/team |
| ------------ | --------------------- |
| jane@acme    | data |
| matthew@acme | data |
| alan@acme    | quant |
| diana@acme   | quant |

In general for read access our users will need `s3:ListBucket` and `s3:GetObject` permissions and for write access our users will additionally need `s3:PutObject` and `s3:DeleteObject`.

Let's create an S3 bucket for our organisation, Acme.
```sh
aws s3 mb s3://acme-arcticdb
```

Then setup the following access policy, save this to `policy.json`.
```
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
```
aws iam creqte-policy --policy-name acme-arcticdb-access --policy-document file://policy.json
```

Take note of the `Arn` in the output as you'll need it to attach the policy to a group
with all your users in, for this example the group is `acme`.
```
aws iam attach-group-policy --policy-arn <ARN> --group-name acme
```

If you intend to adapt that exmaple policy to your own situation then note that,
- `acme-arcticdb` is the name of the bucket and will need to be replaced everywhere
- `s3:ListBucket` is used to permission `ListObjectsV2` and needs its own section, as it applies to the bucket as a whole.  We control access by checking the `s3:prefix` argument that's part of the `ListObjectsV2` request.
- `Put`, `Get` and `Delete` can be specifed for object paths in the second section.
- `_arctic_cfg/cref/*` is where the ArcticDB library configuration is stored and the data for each library is stored in the root of the bucket with a path that starts with the library name.
- By using `${aws:username}` and `${aws:PrincipalTag/team}` we've restricted library access
to those with a matching AWS IAM username or a matched user 'team' tag.

#### Security note

Because `${aws:username}`, `${aws:PrincipalTag/team}` and `_arctic_cfg`... are at the beginning of the path, it's important they don't contain values that can overlap. For example if you have a team called 'data' and a username for an application of 'data', they will have the same permissions, or if a username can be created that `_arctic_cfg`... then that user will be able to modify all library configs.