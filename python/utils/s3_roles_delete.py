from datetime import datetime
import boto3
import os
from .bucket_management import s3_client


def list_roles_by_prefix(client, prefix):
    roles = []
    paginator = client.get_paginator("list_roles")
    for response in paginator.paginate():
        for role in response["Roles"]:
            if role["RoleName"].startswith(prefix):
                roles.append(role["RoleName"])
    return roles


def list_users_by_prefix(client, prefix):
    paginator = client.get_paginator("list_users")
    filtered_users = []

    for page in paginator.paginate():
        for user in page["Users"]:
            if user["UserName"].startswith(prefix):
                filtered_users.append(user["UserName"])

    return filtered_users


def delete_role(iam_client, role_name):
    print("Starting cleanup process...")
    try:
        for policy in iam_client.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
            iam_client.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])
            iam_client.delete_policy(PolicyArn=policy["PolicyArn"])
        print("Policy deleted successfully.")
    except Exception as e:
        print("Error deleting policy")
        print(repr(e))

    # Remove instance profiles
    instance_profiles = iam_client.list_instance_profiles_for_role(RoleName=role_name)["InstanceProfiles"]
    for profile in instance_profiles:
        print(f"Delete {profile}")
        iam_client.remove_role_from_instance_profile(
            InstanceProfileName=profile["InstanceProfileName"], RoleName=role_name
        )
        iam_client.delete_instance_profile(InstanceProfileName=profile["InstanceProfileName"])

    try:
        iam_client.delete_role(RoleName=role_name)
        print("Role deleted successfully.")
    except Exception as e:
        print("Error deleting role")
        print(repr(e))


def delete_user(iam_client, user_name):
    attached_policies = iam_client.list_attached_user_policies(UserName=user_name)["AttachedPolicies"]
    for policy in attached_policies:
        iam_client.detach_user_policy(UserName=user_name, PolicyArn=policy["PolicyArn"])
        print(f"Policy detached: {policy}")
    print("Deleted all inline policies.")

    inline_policies = iam_client.list_user_policies(UserName=user_name)["PolicyNames"]
    for policy in inline_policies:
        iam_client.delete_user_policy(UserName=user_name, PolicyName=policy)
        print(f"Inline policy deleted: {policy}")
    print("Deleted all inline policies.")

    access_keys = iam_client.list_access_keys(UserName=user_name)["AccessKeyMetadata"]
    for key in access_keys:
        iam_client.delete_access_key(UserName=user_name, AccessKeyId=key["AccessKeyId"])
        print(f"Access Key deleted: {key}")
    print("Deleted all access keys.")

    try:
        iam_client.delete_user(UserName=user_name)
        print("User deleted successfully.")
    except Exception as e:
        print("Error deleting user")
        print(repr(e))


PREFIX = os.getenv("ARCTICDB_REAL_S3_STS_PREFIX", "gh_sts_test")

client = s3_client("iam")
roles = list_roles_by_prefix(client, PREFIX)
print(f"Found {len(roles)} roles")
users = list_users_by_prefix(client, PREFIX)
print(f"Found {len(users)} users")

for i, role in enumerate(roles):
    if datetime.today().strftime("%Y-%m-%d") in role:
        print(f"Role {role} is from today, skipping it.")
    else:
        print(f"{i} DELETE role {role}. An old role")
        delete_role(client, role)

for i, user in enumerate(users):
    if datetime.today().strftime("%Y-%m-%d") in user:
        print(f"User {user} is from today, skipping it.")
    else:
        print(f"{i} DELETE user {user}. An old user")
        delete_user(client, user)

roles = list_roles_by_prefix(client, PREFIX)
print(f" {len(roles)} roles remaining")
users = list_users_by_prefix(client, PREFIX)
print(f" {len(users)} users remaining")

max_remaining = 300
assert len(roles) < max_remaining, f"There are at least {max_remaining} out of 1000 roles remaining"
assert len(users) < max_remaining, f"There are at least {max_remaining} out of 1000 users remaining"
print(f"Done")
