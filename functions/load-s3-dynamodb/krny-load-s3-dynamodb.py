import boto3
import csv
import json

from boto3.dynamodb.types import TypeDeserializer
deserializer = TypeDeserializer()

dynamodb = boto3.resource('dynamodb')

file_table_mapping = {
    "yahoo_dynamodb": "krny-yahoo-securities",
    "fred_dynamodb_config": "krny-fred",
    # "googletrends": "krny-google-trends",
    "google_trend_client": "krny-google-trends-client",
    "moodys_dynamodb": "krny-moodys"
}

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        file_name = ''.join(key).split('.')[0]  # convert list to string using join()
        table_name = file_table_mapping.get(file_name)

        if not table_name:
            continue

        ######################
        if file_name == "google_trend_client":
            print("google_trend_client--")
            # Get the object from S3
            obj = s3.get_object(Bucket=bucket, Key=key)

            # Read the JSON file
            data = json.load(obj['Body'])

            # Iterate through the list of items and deserialize each one
            deserialized_items = []
            for item in data:
                deserialized_item = {k: deserializer.deserialize(v) for k, v in item.items()}
                deserialized_items.append(deserialized_item)
                
                # Extract the tenant_id and details
                tenant_id = deserialized_item['tenant_id']
                details = deserialized_item['details']

                # Load the item into DynamoDB
                table.put_item(Item={
                    'tenant_id': tenant_id,
                    'details': details
                })
            print("--google_trend_client: done")
        #########################
        else:
            try:
                s3 = boto3.client('s3')
                file = s3.get_object(Bucket=bucket, Key=key)
                content = file['Body'].read().decode('utf-8').splitlines()
                csv_reader = csv.DictReader(content)
                table = dynamodb.Table(table_name)
                with table.batch_writer() as batch:
                    for row in csv_reader:
                        batch.put_item(Item=row)
            except Exception as e:
                print(f"Error loading data from S3 to DynamoDB table {table_name}: {e}")
                continue

        return {
            'statusCode': 200,
            'body': 'Data loaded successfully'
        }

