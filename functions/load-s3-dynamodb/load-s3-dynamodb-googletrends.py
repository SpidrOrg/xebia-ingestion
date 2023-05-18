# import boto3
# import json
# from boto3.dynamodb.types import TypeDeserializer

# deserializer = TypeDeserializer()

# s3 = boto3.client('s3')
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table('test_google_tenant')

# def lambda_handler(event, context):
#     # Get the bucket and key from the event
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     key = event['Records'][0]['s3']['object']['key']

#     # Get the object from S3
#     obj = s3.get_object(Bucket=bucket, Key=key)

#     # Read the JSON file
#     data = json.load(obj['Body'])

#     # Iterate through the list of items and deserialize each one
#     deserialized_items = []
#     for item in data:
#         deserialized_item = {k: deserializer.deserialize(v) for k, v in item.items()}
#         deserialized_items.append(deserialized_item)
        
#         # Extract the tenant_id and details
#         tenant_id = deserialized_item['tenant_id']
#         details = deserialized_item['details']

#         # Load the item into DynamoDB
#         table.put_item(Item={
#             'tenant_id': tenant_id,
#             'details': details
#         })

#     return {
#         'statusCode': 200,
#         'body': 'Data loaded successfully'
#     }
