import json
from botocore.exceptions import ClientError
from aws_config import *

# NOTE: This could be a possible way to orchestrate policy creation and attack it to its respective roles
# we can have one policy document governing the fm policies, and s3 policy
# to attach oss policy we need to create a collection first

def create_bedrock_kb_execution_role():
    # NOTE: Instead of maintaining separate policies for each resource, we could simply create a single policy with multiple statements in the same policy
    BEDROCK_KB_IAM_POLICIES = [
        {
            'name': 'Bedrock-FM-Policy-KB',
            'arn': None,
            'description': 'Policy for accessing foundation model',
            'document': {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "bedrock:InvokeModel",
                        ],
                        "Resource": [
                            f"arn:aws:bedrock:{REGION}::foundation-model/amazon.titan-embed-text-v2:0"
                        ]
                    }
                ]
            }
        },
        {
            'name': 'Bedrock-S3-Policy-KB',
            'arn': None,
            'description': 'Policy for reading documents from S3',
            'document': {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            f"arn:aws:s3:::{S3_BUCKET}",
                            f"arn:aws:s3:::{S3_BUCKET}/final-players/*"
                        ],
                        "Condition": {
                            "StringEquals": {
                                "aws:ResourceAccount": f"{ACCOUNT_ID}"
                            }
                        }
                    }
                ]
            }
        },
        {
            'name': 'Bedrock-OSS-Policy-KB',
            'arn': None,
            'description': 'Policy for accessing opensearch serverless',
            'document': {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "aoss:APIAccessAll"
                        ],
                        "Resource": [
                            f"arn:aws:aoss:{REGION}:{ACCOUNT_ID}:collection/{AOSS_COLLECTION['id']}"
                        ]
                    }
                ]
            }
        }
    ]
    
    # first, create a clean slate by deleting the bedrock_kb_execution_role and all its attached IAM policies
    delete_iam_execution_role(BEDROCK_KB_EXECUTION_ROLE['name'], BEDROCK_KB_POLICY_NAMES)
    
    # now, create the FM, S3, and OSS IAM policies
    for policy in BEDROCK_KB_IAM_POLICIES:
        try:
            policy['arn'] = iam_client.create_policy(
                PolicyName=policy['name'],
                PolicyDocument=json.dumps(policy['document']),
                Description=policy['description'],
                Tags=TAGS_UPPER_CASE
            )['Policy']['Arn']
            print(f"Successfully created the {policy['name']} IAM policy.")
        except ClientError as e:
            # Check if the error code is 'EntityAlreadyExists'
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"The {policy['name']} IAM policy already exists.")
            else:
              # Re-raise the error if it's not the expected one
                raise  
        
    # create the bedrock kb execution role
    try:
        BEDROCK_KB_EXECUTION_ROLE['arn'] = iam_client.create_role(
            RoleName=BEDROCK_KB_EXECUTION_ROLE['name'],
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "bedrock.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                }
            ),
            Description='Amazon Bedrock Knowledge Base Execution Role for accessing OSS and S3',
            MaxSessionDuration=3600,
            Tags=TAGS_UPPER_CASE
        )['Role']['Arn']
        print(f"Successfully created the {BEDROCK_KB_EXECUTION_ROLE['name']} IAM role.")
    except ClientError as e:
        # Check if the error code is 'EntityAlreadyExists'
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('The Bedrock KB IAM role already exists.')
            BEDROCK_KB_EXECUTION_ROLE['arn'] = iam_client.get_role(RoleName=BEDROCK_KB_EXECUTION_ROLE['name'])['Role']['Arn']
        else:
            # Re-raise the error if it's not the expected one
            raise 
    
    # attach the above created policies to Amazon Bedrock execution role
    for policy in BEDROCK_KB_IAM_POLICIES:
        try:
            iam_client.attach_role_policy(
                RoleName=BEDROCK_KB_EXECUTION_ROLE['name'],
                PolicyArn=policy['arn']
            )
            print(f"Succesfully attached the {policy['name']}.")
        except ClientError as e:
             # Check if the error code is 'EntityAlreadyExists'
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"The {policy['name']} has already been attached to the bedrock KB execution role.")
            else:
                # Re-raise the error if it's not the expected one
                raise 
    
    return

def delete_iam_execution_role(iam_role_name, iam_policies):
    # First, detach all the attached policies to the IAM role
    for policy in iam_policies:
        try:
            iam_client.detach_role_policy(
                RoleName=iam_role_name,
                PolicyArn=f"arn:aws:iam::{ACCOUNT_ID}:policy/{policy}"
            )
            print(f"Successfully detached the {policy} IAM policy from the {iam_role_name} IAM role.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                print(f"Either the IAM role {iam_role_name} or the IAM policy {policy} does not exist.")
            else:
                # Handle any other error
                print(f"An unexpected error occurred: {e}")
                raise

    # Next, delete the Bedrock KB execution role
    try:
        iam_client.delete_role(RoleName=iam_role_name)
        print(f"Successfully deleted the {iam_role_name} IAM role.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print(f"The IAM role {iam_role_name} does not exist.")
        else:
            # Handle any other error
            print(f"An unexpected error occurred: {e}")
            raise
        
    # Finally, delete the IAM policies
    for policy in iam_policies:
        try:
            iam_client.delete_policy(PolicyArn=f"arn:aws:iam::{ACCOUNT_ID}:policy/{policy}")
            print(f"Successfully deleted the {policy} IAM policy.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchEntity':
                print(f"The IAM policy {policy} does not exist.")
            else:
                # Handle any other error
                print(f"An unexpected error occurred: {e}")
                raise
    
    return

def create_oss_policies():
    
    OSS_POLICIES = [
        {
            'name': 'bedrock-rag-encryption-policy',
            'policy': {
                'Rules': [
                    {
                        'Resource': ['collection/' + AOSS_COLLECTION['name']],
                        'ResourceType': 'collection'
                    }
                ],
                'AWSOwnedKey': True
            },
            'type': 'encryption'
        },
        {
            'name': 'bedrock-rag-network-policy',
            'policy': [
                {
                    'Rules': [
                        {
                            'Resource': ['collection/' + AOSS_COLLECTION['name']],
                            'ResourceType': 'collection'
                        }
                    ],
                    'AllowFromPublic': True
                }
            ],
            'type': 'network'
        },
        {
            'name': 'bedrock-rag-access-policy',
            'policy': [   
                {
                    'Rules': [
                        {
                            'Resource': ['collection/' + AOSS_COLLECTION['name']],
                            'Permission': ['aoss:*'],
                            'ResourceType': 'collection'
                        },
                        {
                            'Resource': ['index/' + AOSS_COLLECTION['name'] + '/*'],
                            'Permission': ['aoss:*'],
                            'ResourceType': 'index'
                        }
                    ],
                    'Principal': [IDENTITY, BEDROCK_KB_EXECUTION_ROLE['arn']],
                    'Description': 'Easy data policy'
                }
            ],
            'type': 'data'
        }
    ]
    
    # first, create a clean slate by deleting all the OSS policies
    delete_oss_policies()
    
    # Traverse through all the OSS Policies and accordingly create them
    for policy in OSS_POLICIES:
        try:
            if policy['name'] != 'bedrock-rag-access-policy':
                aoss_client.create_security_policy(
                    name=policy['name'],
                    policy=json.dumps(policy['policy']),
                    type=policy['type']
                )
            else:
                aoss_client.create_access_policy(
                    name=policy['name'],
                    policy=json.dumps(policy['policy']),
                    type=policy['type']
                )
            print(f"Successfully created the AOSS {policy['name']}.")
        except ClientError as e:
            # Check for ConflictException
            if e.response['Error']['Code'] == 'ConflictException':
                print(f"{policy['name']} already exists.")
            else:
                # Handle other exceptions
                raise
    
    return

def delete_oss_policies():
    for policy in AOSS_POLICY_NAMES:
        try:
            if policy['name'] != 'bedrock-rag-access-policy':
                # Delete this OSS policy
                aoss_client.delete_security_policy(
                    name=policy['name'],
                    type=policy['type']  # 'encryption', 'network', or 'data' for security; 'access' for access policies
                )
            else:
                aoss_client.delete_access_policy(
                    name=policy['name'],
                    type=policy['type']  # 'encryption', 'network', or 'data' for security; 'access' for access policies
                )
            print(f"Deleted the AOSS {policy['name']}.")
            pass
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print(f"The OSS policy {policy['name']} does not exist.")
            else:
                # Handle any other error
                print(f"An unexpected error occurred: {e}")
                raise
    pass

def create_bedrock_agent_execution_role():
    # NOTE: Instead of maintaining separate policies for each resource, we could simply create a single policy with multiple statements in the same policy
    BEDROCK_AGENT_IAM_POLICIES = [
        {
            'name': 'Bedrock-FM-Policy-Agent',
            'arn': None,
            'description': 'Policy for accessing foundation model',
            'document': {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "bedrock:InvokeModel",
                        ],
                        "Resource": [
                            # Anthropic Claude 3 Sonnet
                            f"arn:aws:bedrock:{REGION}::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0",
                            # Anthropic Claude 3 Haiku
                            f"arn:aws:bedrock:{REGION}::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"
                        ]
                    }
                ]
            }
        },
        {
            'name': 'Bedrock-KB-Policy-Agent',
            'arn': None,
            'description': 'Policy for accessing the Bedrock Knowledge Base.',
            'document': {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "bedrock:Retrieve",
                            "bedrock:RetrieveAndGenerate"
                        ],
                        "Resource": [
                            f"arn:aws:bedrock:{REGION}:{ACCOUNT_ID}:knowledge-base/{BEDROCK_KB['id']}"
                        ]
                    }
                ]
            }
        }
    ]
    
    # first, create a clean slate by deleting the bedrock_agent_execution_role and all its attached IAM policies
    delete_iam_execution_role(BEDROCK_AGENT_EXECUTION_ROLE['name'], BEDROCK_AGENT_POLICY_NAMES)
    
    # now, create the FM and KB IAM policies
    for policy in BEDROCK_AGENT_IAM_POLICIES:
        try:
            policy['arn'] = iam_client.create_policy(
                PolicyName=policy['name'],
                PolicyDocument=json.dumps(policy['document']),
                Description=policy['description'],
                Tags=TAGS_UPPER_CASE
            )['Policy']['Arn']
            print(f"Successfully created the {policy['name']} IAM policy.")
        except ClientError as e:
            # Check if the error code is 'EntityAlreadyExists'
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"The {policy['name']} IAM policy already exists.")
            else:
              # Re-raise the error if it's not the expected one
                raise  
    
    # create the bedrock agent execution role
    try:
        BEDROCK_AGENT_EXECUTION_ROLE['arn'] = iam_client.create_role(
            RoleName=BEDROCK_AGENT_EXECUTION_ROLE['name'],
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "bedrock.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                }
            ),
            Description='Amazon Bedrock Agent Execution Role for accessing the FMs and Bedrock KB.',
            MaxSessionDuration=3600,
            Tags=TAGS_UPPER_CASE
        )['Role']['Arn']
        print(f"Successfully created the {BEDROCK_AGENT_EXECUTION_ROLE['name']} IAM role.")
    except ClientError as e:
        # Check if the error code is 'EntityAlreadyExists'
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('The Bedrock Agent IAM role already exists.')
            BEDROCK_AGENT_EXECUTION_ROLE['arn'] = iam_client.get_role(RoleName=BEDROCK_AGENT_EXECUTION_ROLE['name'])['Role']['Arn']
        else:
            # Re-raise the error if it's not the expected one
            raise 
    
    # attach the above created policies to Amazon Bedrock Agent execution role
    for policy in BEDROCK_AGENT_IAM_POLICIES:
        try:
            iam_client.attach_role_policy(
                RoleName=BEDROCK_AGENT_EXECUTION_ROLE['name'],
                PolicyArn=policy['arn']
            )
            print(f"Succesfully attached the {policy['name']}.")
        except ClientError as e:
             # Check if the error code is 'EntityAlreadyExists'
            if e.response['Error']['Code'] == 'EntityAlreadyExists':
                print(f"The {policy['name']} has already been attached to the bedrock agent execution role.")
            else:
                # Re-raise the error if it's not the expected one
                raise 
    
    return