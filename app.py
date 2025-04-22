import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from io import StringIO
import datetime

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


csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

bucket="tipospot"

fecha = datetime.datetime.now().strftime("%Y%m%d")
df.to_csv(f"s3://{bucket}/spot_rates_{fecha}.csv", index=False)


