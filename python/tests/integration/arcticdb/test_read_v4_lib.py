from arcticdb import Arctic


## Libs below were created with aws_auth=True on arcticdb==4.3.1


def test_rbac():
    ac = Arctic("s3://s3.eu-west-2.amazonaws.com:alex-test-bucket4?access=_RBAC_&secret=_RBAC_&force_uri_lib_config=True")
    lib = ac["tst"]
    assert lib.list_symbols() == ["abc"]


def test_aws_auth():
    ac = Arctic("s3://s3.eu-west-2.amazonaws.com:alex-test-bucket4?aws_auth=True&force_uri_lib_config=True")
    lib = ac["tst"]
    assert lib.list_symbols() == ["abc"]


def test_no_force():
    ac = Arctic("s3://s3.eu-west-2.amazonaws.com:alex-test-bucket4?aws_auth=True")
    lib = ac["tst"]
    assert lib.list_symbols() == ["abc"]


def test_explicit_creds():
    ac = Arctic("s3://s3.eu-west-2.amazonaws.com:alex-test-bucket7?access=REDACTED&secret=REDACTED")
    lib = ac["one"]
    assert lib.list_symbols() == ["abc"]
