# Getting Started with AWS S3

There are detailed guides on the AWS website on S3 configuration.  You can start with the [Creating your first bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-bucket.html) page.

Best practice can depend on your situation.  If your looking to try our ArcticDB on AWS S3 then a reasonably simple approach is the following.

### 1. Create an IAM user

Best practice is to manage and access S3 buckets with a Non-root IAM account in AWS.   It's not nessecery, but ideally create one just for trialling ArcticDB.

You can create an IAM account at [IAM > Users](https://console.aws.amazon.com/iamv2/home#/users).
- Come up with a username, e.g. `arcticdbtrial`
- Attach the `AmazonS3FullAccess` policy to that account.
- Click create.

### 2. Create the Access key

Create a new access key,
- Click on the IAM user account from the [IAM > Users](https://console.aws.amazon.com/iamv2/home#/users) table.
- Go to Security credentials > Access keys and click `Create access Key`
- Select `Local code` option, ArcticDB is the local code.
- Check `I understand...` and click `Next`.
- Click `Create Access Key`.
- Record the Access key and Secret access key somewhere secure.

### 3. Create the bucket

The rest of the steps can be performed using commands on your client machine.  Install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) if needed.

Configure your client machine to use the new account with the Access key and Secret access key.
You will also need to select an [AWS region](https://docs.aws.amazon.com/general/latest/gr/rande.html).
Selecting a region that is local to your ArcticDB client will improve performance.
```
aws configure
```

Create a new bucket.  Bucket names must be globally unique so you will need to come up with your own.
```
aws s3 mb s3://arcticdbtrial
```

### 4. Connect to the bucket

Using the example of `eu-west-2` for the region and `arcticdbtrial` for the bucket with authentication setup provided by `aws configure`.
```python
from arcticdb import Arctic
arctic = Arctic('s3://s3.eu-west-2.amazonaws.com:arcticdbtrial?aws_auth=true')
```

## Checking connectivity for your S3 bucket.

ArcticDB currently uses five S3 methods, those are:
- GetObject
- PutObject
- HeadObject
- DeleteObject
- ListObjectsV2

Here is a short script you can use to check connectivity to a bucket from your client.  If this works, then the configuration should be correct for read and write with ArcticDB.

Replace `YOUR_BUCKET_NAME` below.  This script assumes you've setup `~/.aws/credentials` with `aws configure`.  
```python
import io
import boto3
s3 = boto3.client('s3')
bucket = 'YOUR_BUCKET_NAME'
s3.put_object(Bucket=bucket, Key='_arctic_check/check.txt', Body=io.BytesIO(b'check file contents'))
s3.list_objects_v2(Bucket=bucket, Prefix='_arctic_check/')
s3.head_object(Bucket=bucket, Key='_arctic_check/check.txt')
s3.get_object(Bucket=bucket, Key='_arctic_check/check.txt')
s3.delete_object(Bucket=bucket, Key='_arctic_check/check.txt')
```
The check object written in that script should not interfere with normal ArcticDB operation on the bucket.
