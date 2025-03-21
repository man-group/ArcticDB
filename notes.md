## STS setup notes

```~/.aws/config
[default]
region = eu-west-2

[profile aseaton_profile]
role_arn = arn:aws:iam::459367462176:role/aseaton-s3-tst
source_profile = base_profile

[profile base_profile]
aws_access_key_id = 
aws_secret_access_key = 
```

## Hacks

(310-4) ➜  sts sudo ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt
(310-4) ➜  sts sudo less /etc/pki/tls/certs/ca-bundle.crt
export ARCTICDB_S3Storage_VerifySSL_int=0

## API

```
ac = Arctic("s3://s3.eu-west-2.amazonaws.com:arcticdb-repl-test-202404-target-1?aws_auth=sts&aws_profile=aseaton_profile")
```
