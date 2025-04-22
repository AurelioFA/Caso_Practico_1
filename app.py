import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from io import StringIO
from decimal import Decimal
from datetime import datetime

def handler(event, context):
    # Scraping del BCE
    url = "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(response.content, "html.parser")

    # Extraer tabla de divisas
    table = soup.find("table", class_="forextable")
    data = []

    for row in table.find("tbody").find_all("tr"):
        cols = row.find_all("td")
        if len(cols) >= 2:
            currency = cols[0].text.strip()
            spot = cols[2].text.strip()
            data.append({"Currency": currency, "Spot": spot})

    df = pd.DataFrame(data)
    print(df.to_string(index=False))  

    # Guardar CSV en memoria
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Subir a S3
    s3 = boto3.client("s3")
    bucket = "tipospot"
    fecha = datetime.now().strftime("%Y%m%d")
    filename = f"spot_rates_{fecha}.csv"

    s3.put_object(
        Bucket=bucket,
        Key=filename,
        Body=csv_buffer.getvalue()
    )

    # Preparar para DynamoDB
    df.rename(columns={"Currency": "CURRENCY", "Spot": "TipoSpot"}, inplace=True)
    df["TIME"] = fecha

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("TipoSpot")
    records = df.to_dict(orient="records")

    for record in records:
        record["TipoSpot"] = Decimal(str(record.get("TipoSpot")))
        table.put_item(Item=record)

    return {
        'statusCode': 200,
        'body': f"✔️ Subido a S3 y DynamoDB: {filename}"
    }
