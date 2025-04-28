from datetime import datetime
import boto3
import os


def boto_client():
    return boto3.client(
        "iam",
        aws_access_key_id=os.getenv("ARCTICDB_REAL_S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("ARCTICDB_REAL_S3_SECRET_KEY"),
    )

def list_roles_by_prefix(client, prefix):
    roles = []
    paginator = client.get_paginator('list_roles')
    for response in paginator.paginate():
        for role in response['Roles']:
            if role['RoleName'].startswith(prefix):
                roles.append(role['RoleName'])
    return roles

def delete_role(iam_client, role_name):
    print("Starting cleanup process...")
    try:
        for policy in iam_client.list_attached_role_policies(RoleName=role_name)["AttachedPolicies"]:
            iam_client.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])
            iam_client.delete_policy(PolicyArn=policy["PolicyArn"])
        print("Policy deleted successfully.")
    except Exception:
        print("Error deleting policy")

    # Remove instance profiles
    instance_profiles = iam_client.list_instance_profiles_for_role(RoleName=role_name)['InstanceProfiles']
    for profile in instance_profiles:
        print(f"Delete {profile}")
        iam_client.remove_role_from_instance_profile(InstanceProfileName=profile['InstanceProfileName'], RoleName=role_name)
        iam_client.delete_instance_profile(InstanceProfileName=profile['InstanceProfileName'])        

    try:
        iam_client.delete_role(RoleName=role_name)
        print("Role deleted successfully.")
    except Exception:
        print("Error deleting role")  # Role could be non-existent as creation of it may fail

prefix = "gh_sts_test_role_" # This is the template name prefix of roles that GH runners are creating
client = boto_client()
roles = list_roles_by_prefix(client, prefix)
print(f"Found {len(roles)} roles")

for i, role in enumerate(roles):
    if datetime.today().strftime('%Y-%m-%d') in role:
        print(f"Role {role} is from today, skipping it.")
    else:
        print(f"{i} DELETE role {role}. An old role")
        delete_role(client, role)

roles = list_roles_by_prefix(client, prefix)
print(f" {len(roles)} roles remaining")
print(f"Done")    
