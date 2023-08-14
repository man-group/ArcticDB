# Getting Started with AWS S3

There are detailed guides on the AWS website on S3 configuration.  You can start with the [Creating your first bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-bucket.html) page.

Best practices with AWS S3 can depend on your situation.  If you're looking to try ArcticDB on AWS S3 then a reasonably simple approach is the following.

### 1. Create an IAM user

Best practice is to manage and access S3 buckets with a Non-root IAM account in AWS.   Ideally create a new account just for trialling ArcticDB.

You can create an IAM account at [IAM > Users](https://console.aws.amazon.com/iamv2/home#/users).
- Click `Add users`.
- Come up with a username, e.g. `arcticdbtrial`, then click `Next`.
- Select `Attach policies directly` and attach the `AmazonS3FullAccess` policy to that account, then click `Next`.
- Click `Create user`.

### 2. Create the access key

- Click on the new user in the [IAM > Users](https://console.aws.amazon.com/iamv2/home#/users) table.
- Go to `Security credentials` > `Access keys` and click `Create access Key`
- Select the `Local code` option, ArcticDB is the local code.
- Check `I understand...` and click `Next`.
- Click `Create access Key`.
- Record the Access key and Secret access key somewhere securely.

### 3. Create the bucket

The rest of the steps can be performed using commands on your client machine. Install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) if you do not already have it installed.

Use the AWS CLI to configure your client machine with the Access key and Secret access key.
You will also need to select an [AWS region](https://docs.aws.amazon.com/general/latest/gr/rande.html).
Selecting a region that is local to your ArcticDB client will improve performance.
```bash
$ aws configure
AWS Access Key ID [None]: <ACCESS_KEY>
AWS Secret Access Key [None]: <SECRET_KEY>
Default region name [None]: <REGION>
Default output format [None]:
```

Create a new bucket.  Bucket names must be globally unique so you will need to come up with your own.
```bash
$ aws s3 mb s3://<BUCKET_NAME>
```

### 4. Connect to the bucket

- [Install ArcticDB](https://github.com/man-group/ArcticDB#readme).
- Use your `<REGION>` and `<BUCKET_NAME>`.
- Setup `~/.aws/credentials` with `aws configure`, as above.
```python
from arcticdb import Arctic
arctic = Arctic('s3://s3.<REGION>.amazonaws.com:<BUCKET_NAME>?aws_auth=true')
```

## Checking connectivity to the S3 bucket.

ArcticDB currently uses five S3 methods, those are:
- GetObject
- PutObject
- HeadObject
- DeleteObject
- ListObjectsV2

Here is a short script you can use to check connectivity to a bucket from your client.  If this works, then the configuration should be correct for read and write with ArcticDB.

- Use your `<BUCKET_NAME>` below.  
- Install [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#installation).
- Setup `~/.aws/credentials` with `aws configure`, as above.  
```python
import io
import boto3
s3 = boto3.client('s3')
bucket = '<BUCKET_NAME>'
s3.put_object(Bucket=bucket, Key='_arctic_check/check.txt', Body=io.BytesIO(b'check file contents'))
s3.list_objects_v2(Bucket=bucket, Prefix='_arctic_check/')
s3.head_object(Bucket=bucket, Key='_arctic_check/check.txt')
s3.get_object(Bucket=bucket, Key='_arctic_check/check.txt')
s3.delete_object(Bucket=bucket, Key='_arctic_check/check.txt')
```
The check object written in that script should not interfere with normal ArcticDB operation on the bucket.
