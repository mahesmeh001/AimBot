import gzip
from io import BytesIO
import boto3
import botocore
from botocore.errorfactory import ClientError

s3Client = boto3.client('s3')

SOURCE_S3_BUCKET = "vcthackathon-data"
DESTINATION_S3_BUCKET = "esports-digital-assistant-data"

# Function to list and transfer objects
def transfer_s3_objects():
    # List objects in the source bucket
    paginator = s3Client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=SOURCE_S3_BUCKET)
    
    for page in page_iterator:
        if "Contents" in page:
            for object in page["Contents"]:
                key = object['Key']

                try:
                    if key[-3:] == '.gz':
                        s3Client.head_object(Bucket=DESTINATION_S3_BUCKET, Key=key[:-3])
                    else:
                        s3Client.head_object(Bucket=DESTINATION_S3_BUCKET, Key=key)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":

                        if key[-3:] == '.gz':
                            # Download the gzip file from the source S3 bucket
                            gzip_obj = s3Client.get_object(Bucket=SOURCE_S3_BUCKET, Key=key)
                            gzip_content = gzip_obj['Body'].read()

                            # Unzip the content
                            with gzip.GzipFile(fileobj=BytesIO(gzip_content), mode='rb') as gzip_file:
                                # Upload the unzipped content to the destination S3 bucket
                                s3Client.put_object(Bucket=DESTINATION_S3_BUCKET, Key=key[:-3], Body=gzip_file.read())
                                print(f"Uploaded unzipped file to {DESTINATION_S3_BUCKET}/{key[:-3]}")
                        else:
                            s3Client.copy(
                                {
                                    'Bucket': SOURCE_S3_BUCKET,
                                    'Key': key
                                }, 
                                Bucket=DESTINATION_S3_BUCKET,
                                Key=key)
                            print(f"Uploaded non-gzip file {key} from {SOURCE_S3_BUCKET} to {DESTINATION_S3_BUCKET}")


if __name__ == "__main__":
    # Call the function to start the transfer
    transfer_s3_objects()
