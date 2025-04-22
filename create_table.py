import boto3

dynamodb = boto3.resource('dynamodb')

table = dynamodb.create_table(
    TableName='TipoSpot',
    KeySchema=[
        {
            'AttributeName': 'CURRENCY',
            'KeyType': 'HASH'  # Partition key
        },
        {
            'AttributeName': 'TIME',
            'KeyType': 'RANGE'  # Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'CURRENCY',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'TIME',
            'AttributeType': 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)
