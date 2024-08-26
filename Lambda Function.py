import json
import boto3
import urllib.parse

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('lab3-depii')

def lambda_handler(event, context):
    # Parse the S3 event
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        print('bucket name : ', bucket)
        # Copy the file to the new bucket
        copy_source = {'Bucket': bucket, 'Key': key}
        destination_bucket = 'lec3-depi-copy'
        
        s3_client.copy_object(
            CopySource=copy_source,
            Bucket=destination_bucket,
            Key=key
        )
        
        # Log metadata to DynamoDB
        table.put_item(
            Item={
                'FileName': key,
                'SourceBucket': bucket,
                'DestinationBucket': destination_bucket
            }
        )
        
    return {
        'statusCode': 200,
        'body': json.dumps('File copied and metadata logged successfully')
    }