import time
from botocore.exceptions import ClientError, ParamValidationError
from iam_manager import delete_iam_execution_role, delete_oss_policies
from aws_config import *

def delete_bedrock_agent():
    # Loop through the agents to find the one with the matching name
    for agent in bedrock_agent_client.list_agents()['agentSummaries']:
        if agent['agentName'] == BEDROCK_AGENT['name']:
            # Return agent ID if found
            BEDROCK_AGENT['id'] = agent['agentId']

    for knowledge_base in bedrock_agent_client.list_knowledge_bases()['knowledgeBaseSummaries']:
        if knowledge_base['name'] == BEDROCK_KB['name']:
            # Return knowledge base ID if found
            BEDROCK_KB['id'] = knowledge_base['knowledgeBaseId']
    
    try:
        bedrock_agent_client.disassociate_agent_knowledge_base(
            agentId = BEDROCK_AGENT['id'],
            agentVersion = 'DRAFT',
            knowledgeBaseId = BEDROCK_KB['id']
        )
        bedrock_agent_client.delete_agent(agentId = BEDROCK_AGENT['id'])
        time.sleep(10)
        print(f"Successfully deleted the Bedrock Agent {BEDROCK_AGENT['name']}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntityException':
            print(f"The agent {BEDROCK_AGENT['name']} does not exist.")
        else:
            # Handle any other error
            print(f"An unexpected error occurred: {e}")
            raise
    except ParamValidationError as e:
        print(f"The agent {BEDROCK_AGENT['name']} or knowledge base {BEDROCK_KB['name']} does not exist.")    
    
    # delete respective bedrock KB execution role
    delete_iam_execution_role(BEDROCK_AGENT_EXECUTION_ROLE['name'], BEDROCK_AGENT_POLICY_NAMES)
    
    return

def delete_data_source():
    try:
        # Loop through the data sources to find the one with the matching name
        for data_source in bedrock_agent_client.list_data_sources(knowledgeBaseId=BEDROCK_KB['id'])['dataSourceSummaries']:
            if data_source['name'] == BEDROCK_KB_DATA_SOURCE['name']:
                # Return agent ID if found
                BEDROCK_KB_DATA_SOURCE['id'] = data_source['dataSourceId']
        
        bedrock_agent_client.delete_data_source(
            knowledgeBaseId = BEDROCK_KB['id'],
            dataSourceId = BEDROCK_KB_DATA_SOURCE['id']
        )
        time.sleep(10)
        print(f"Successfully deleted the data source {BEDROCK_KB_DATA_SOURCE['name']}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntityException':
            print(f"The data source {BEDROCK_KB_DATA_SOURCE['name']} or knowledge base {BEDROCK_KB['name']} does not exist.")
        else:
            # Handle any other error
            print(f"An unexpected error occurred: {e}")
            raise
    except ParamValidationError as e:
        print(f"The data source {BEDROCK_KB_DATA_SOURCE['name']} or knowledge base {BEDROCK_KB['name']} does not exist.")
    
    return

def delete_bedrock_knowledge_base():
    try:
        bedrock_agent_client.delete_knowledge_base(knowledgeBaseId = BEDROCK_KB['id'])
        time.sleep(10)
        print(f"Successfully deleted the Bedrock Knowledge Base {BEDROCK_KB['name']}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntityException':
            print(f"The Knowledge Base {BEDROCK_KB['name']} does not exist.")
        else:
            # Handle any other error
            print(f"An unexpected error occurred: {e}")
            raise
    except ParamValidationError as e:
        print(f"The Knowledge Base {BEDROCK_KB['name']} does not exist.")    
    
    # delete respective bedrock KB execution role
    delete_iam_execution_role(BEDROCK_KB_EXECUTION_ROLE['name'], BEDROCK_KB_POLICY_NAMES)
    
    return

def delete_aoss_vector_store():
    # Loop through the AOSS collections to find the one with the matching name
    for collection in aoss_client.list_collections()['collectionSummaries']:
        if collection['name'] == AOSS_COLLECTION['name']:
                AOSS_COLLECTION['id'] = collection['id']
    
    try:
        aoss_client.delete_collection(id=AOSS_COLLECTION['id'])
        time.sleep(10)
        print(f"Successfully deleted the AOSS Collection {AOSS_COLLECTION['name']}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntityException':
            print(f"The AOSS Collection {AOSS_COLLECTION['name']} does not exist.")
        else:
            # Handle any other error
            print(f"An unexpected error occurred: {e}")
            raise
    except ParamValidationError as e:
        print(f"The AOSS Collection {AOSS_COLLECTION['name']} does not exist.")
    
    # delete respective OSS policies
    delete_oss_policies()
    
    return

def main():
    # delete the bedrock agent
    delete_bedrock_agent()
    # delete all data sources attached to the Bedrock KB
    delete_data_source()
    # delete the Bedrock KB
    delete_bedrock_knowledge_base()
    # delete the associating AOSS vector store
    delete_aoss_vector_store()
    
    return

if __name__ == '__main__':
    main()
