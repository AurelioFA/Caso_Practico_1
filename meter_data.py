import pandas as pd
import boto3
from io import StringIO
from decimal import Decimal
import datetime


s3 = boto3.client("s3")
bucket = "tipospot"
fecha = datetime.datetime.now().strftime("%Y%m%d")
filename = f"spot_rates_{fecha}.csv"

obj = s3.get_object(Bucket=bucket, Key=filename)
csv_data = obj["Body"].read().decode("utf-8")

df = pd.read_csv(StringIO(csv_data))
print(df)

# Insertar en DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("TipoSpot")

with table.batch_writer() as writer:
    for _, row in df.iterrows():
        item = {
            "CURRENCY": row["Currency"],
            "TIME": fecha,
            "TipoSpot": Decimal(str(row["Spot"]))
        }
        writer.put_item(Item=item)
