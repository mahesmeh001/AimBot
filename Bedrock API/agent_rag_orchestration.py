import json
import time
from botocore.exceptions import ClientError
from iam_manager import create_bedrock_agent_execution_role, create_bedrock_kb_execution_role, create_oss_policies
from retrying import retry
from opensearchpy import OpenSearch, RequestsHttpConnection, RequestError
from aws_config import *

def interactive_sleep(seconds):
    dots = ''
    for i in range(seconds):
        dots += '*'
        print(dots, end='\r')
        time.sleep(1)
    return

# Function definition to create Bedrock Execution Role and initialize all its relevant permissions
# and finally create an OpenSearch serverless collection
def create_aoss_vector_store():
    # Check if bucket exists
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f'Bucket {S3_BUCKET} exists.')
    except ClientError as e:
        print(f'Bucket {S3_BUCKET} does not exist. Please run ETL/s3_etl_pipeline.py to create this bucket.')

    # Create an AOSS collection if it does not exist already
    try:
        # Try to create the collection
        collection = aoss_client.create_collection(name=AOSS_COLLECTION['name'], type='VECTORSEARCH', tags=TAGS_LOWER_CASE)
        AOSS_COLLECTION['id'] = collection['createCollectionDetail']['id']
        AOSS_COLLECTION['arn'] = collection['createCollectionDetail']['arn']
        
        # wait for collection creation
        # This can take couple of minutes to finish
        # Periodically check collection status
        while aoss_client.batch_get_collection(names=[AOSS_COLLECTION['name']])['collectionDetails'][0]['status'] == 'CREATING':
            print('Creating collection...')
            interactive_sleep(30)
        print(f"Collection {AOSS_COLLECTION['name']} created successfully.")
    except ClientError as e:
        # Handle the case where the collection already exists
        if e.response['Error']['Code'] == 'ConflictException':
            print(f"Collection '{AOSS_COLLECTION['name']}' already exists. No action needed.")
            
            collection = aoss_client.batch_get_collection(names=[AOSS_COLLECTION['name']])
            AOSS_COLLECTION['id'] = collection['collectionDetails'][0]['id']
            AOSS_COLLECTION['arn'] = collection['collectionDetails'][0]['arn']
        else:
            # Re-raise the exception for other errors
            raise
    
    # TODO: a simple bedrock kb execution role needs to be created before creating an AOSS collection (w/ s3 and fm policies)
    # Then, we attach the OSS policy to the role after the collection has been created
    # Consequently, we have to create the OSS policies before the actual collection creation
    # Creating bedrock execution role with relevant S3, Bedrock FM, and AOSS access
    create_bedrock_kb_execution_role()
    # Creating security, network and data access policies within OSS (handles deletion of existing policies as well)
    create_oss_policies()
    
    return

# Function definition to create the vector index for the AOSS collection
def create_aoss_vector_index():

    # Build the OpenSearch client
    oss_client = OpenSearch(
        hosts=[{'host': AOSS_COLLECTION['id'] + '.' + REGION + '.aoss.amazonaws.com', 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=300
    )
    
    body_json = {
        "settings": {
            "index.knn": "true",
            "number_of_shards": 1,
            "knn.algo_param.ef_search": 512,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "vector": {
                    "type": "knn_vector",
                    "dimension": 1536,
                    "method": {
                        "name": "hnsw",
                        "engine": "faiss",
                        "space_type": "l2"
                    },
                },
                "text": {
                    "type": "text"
                },
                "text-metadata": {
                    "type": "text"
                }
            }
        }
    }
    
    # Create vector index
    try:
        oss_client.indices.create(index=INDEX_NAME, body=json.dumps(body_json))
        # index creation can take up to a minute
        interactive_sleep(60)
        print(f'Successfully created the {INDEX_NAME} with AOSS.')
    except RequestError as e:
        # you can delete the index if its already exists
        print(f'{INDEX_NAME} already exists. No action needed.')
    
    return

# Function definition to create the knowledge base and associate it with the AOSS collection
@retry(wait_random_min=1000, wait_random_max=2000,stop_max_attempt_number=7)
def create_bedrock_knowledge_base():
    
    try:
        knowledge_base = bedrock_agent_client.create_knowledge_base(
            name = BEDROCK_KB['name'],
            description = BEDROCK_KB['description'],
            roleArn = BEDROCK_KB_EXECUTION_ROLE['arn'],
            knowledgeBaseConfiguration = {
                "type": "VECTOR",
                "vectorKnowledgeBaseConfiguration": {
                    "embeddingModelArn": f"arn:aws:bedrock:{REGION}::foundation-model/amazon.titan-embed-text-v2:0",
                    'embeddingModelConfiguration': {
                        'bedrockEmbeddingModelConfiguration': {
                            'dimensions': 1024
                        }
                    }
                }
            },
            storageConfiguration = {
                "type": "OPENSEARCH_SERVERLESS",
                "opensearchServerlessConfiguration": {
                    "collectionArn": AOSS_COLLECTION['arn'],
                    "vectorIndexName": INDEX_NAME,
                    "fieldMapping": {
                        "vectorField": "vector",
                        "textField": "text",
                        "metadataField": "text-metadata"
                    }
                }
            },
            tags=TAGS_DICT
        )
        
        BEDROCK_KB['id'] = knowledge_base['knowledgeBase']['knowledgeBaseId']
        BEDROCK_KB['arn'] = knowledge_base['knowledgeBase']['knowledgeBaseArn']

        # Get knowledge base status
        while knowledge_base['knowledgeBase']['status'] == 'CREATING' :
            knowledge_base = bedrock_agent_client.get_knowledge_base(knowledgeBaseId = BEDROCK_KB['id']) 
            interactive_sleep(5)
            
        print(f"Successfully created the {BEDROCK_KB['name']} Knowledge Base.")
    except ClientError as e:
        # you can delete the KB if its already exists
        if e.response['Error']['Code'] == 'ConflictException':
            
            # TODO: Search for the knowledge base
            BEDROCK_KB['id'] = knowledge_base['knowledgeBase']['knowledgeBaseId']
            BEDROCK_KB['arn'] = knowledge_base['knowledgeBase']['knowledgeBaseArn']
    
    return
    
def create_data_source():
    
    try:
        data_source = bedrock_agent_client.create_data_source(
            name = BEDROCK_KB_DATA_SOURCE['name'],
            description = BEDROCK_KB_DATA_SOURCE['description'],
            knowledgeBaseId = BEDROCK_KB['id'],
            dataSourceConfiguration = {
                "type": "S3",
                "s3Configuration": {
                    "bucketArn": f"arn:aws:s3:::{S3_BUCKET}",
                    # "inclusionPrefixes":["*.*"] # you can use this if you want to create a KB using data within s3 prefixes.
                }
            },
            vectorIngestionConfiguration = {
                "chunkingConfiguration": {
                    "chunkingStrategy": "FIXED_SIZE",
                    "fixedSizeChunkingConfiguration": {
                        "maxTokens": 512,
                        "overlapPercentage": 20
                    }
                }
            }
        )
        
        BEDROCK_KB_DATA_SOURCE['id'] = data_source['dataSource']['dataSourceId']
        
        # Get data source status
        while data_source['dataSource']['status'] == 'CREATING' :
            data_source = bedrock_agent_client.get_data_source(knowledgeBaseId = BEDROCK_KB['id'], dataSourceId = BEDROCK_KB_DATA_SOURCE['id']) 
            interactive_sleep(5)
        
        print(f"Successfully created the {BEDROCK_KB_DATA_SOURCE['name']} data source for the {BEDROCK_KB['name']} Knowledge Base.")
    except ClientError as e:
        # you can delete the data source if its already exists
        if e.response['Error']['Code'] == 'ConflictException':
            print(f"{BEDROCK_KB_DATA_SOURCE['name']} already exists. No action needed.")
    
    return

def start_ingestion_job():
    job = bedrock_agent_client.start_ingestion_job(
        knowledgeBaseId = BEDROCK_KB['id'],
        dataSourceId = BEDROCK_KB_DATA_SOURCE['id']
    )
    
    # Get job 
    while job['ingestionJob']['status'] in ['STARTING', 'IN PROGRESS']:
        job = bedrock_agent_client.get_ingestion_job(
            knowledgeBaseId = BEDROCK_KB['id'],
            dataSourceId = BEDROCK_KB_DATA_SOURCE['id'],
            ingestionJobId = job["ingestionJobId"]
        )
        
        interactive_sleep(30)
    
    print(f"Successfully synced data from data source {BEDROCK_KB_DATA_SOURCE['name']}")
    
    return

def create_bedrock_agent():
    # Creating bedrock agent execution role with relevant Bedrock FM and KB access
    create_bedrock_agent_execution_role()
    
    try:
        agent = bedrock_agent_client.create_agent(
            agentName = BEDROCK_AGENT['name'],
            description = BEDROCK_AGENT['description'],
            instruction = BEDROCK_AGENT['instruction'],
            agentResourceRoleArn = BEDROCK_AGENT_EXECUTION_ROLE['arn'],
            idleSessionTTLInSeconds=1800,
            foundationModel = FOUNDATION_MODEL,
            tags=TAGS_DICT
        )
        
        BEDROCK_AGENT['id'] = agent['agent']['agentId']
        BEDROCK_AGENT['arn'] = agent['agent']['agentArn']
        BEDROCK_AGENT['version'] = agent['agent']['agentVersion']
        
        # Get knowledge base status
        while agent['agent']['status'] == 'CREATING' :
            agent = bedrock_agent_client.get_agent(agentId = BEDROCK_AGENT['id']) 
            interactive_sleep(5)
            
        print(f"Successfully created the {BEDROCK_AGENT['name']} Agent.")
        
        # Associate the bedrock associate to the created knowledge base
        bedrock_agent_client.associate_agent_knowledge_base(
            agentId = BEDROCK_AGENT['id'],
            agentVersion = 'string',
            description = '',
            knowledgeBaseId = BEDROCK_KB['id'],
            knowledgeBaseState = 'ENABLED'
        )
    except ClientError as e:
        # you can delete the agent if it already exists
        if e.response['Error']['Code'] == 'ConflictException':
            print(f"{BEDROCK_AGENT['name']} already exists. No action needed.")
            
            # BEDROCK_KB['id'] = knowledge_base['knowledgeBase']['knowledgeBaseId']
            # BEDROCK_KB['arn'] = knowledge_base['knowledgeBase']['knowledgeBaseArn']
    pass
  
def main():
    # create the OpenSearch collection
    create_aoss_vector_store()
    # create vector index in OpenSearch serverless
    create_aoss_vector_index()
    # create knowledge base in AWS Bedrock
    create_bedrock_knowledge_base()
    # create data source in the Bedrock KB
    create_data_source()
    # start an ingestion job for the data source to start syncing data
    start_ingestion_job()
    # create bedrock agent and associate the knowledge base to this agent
    create_bedrock_agent()
    
    return
    
if __name__ == "__main__":
    main()

