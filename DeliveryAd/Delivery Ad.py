from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
import pandas as pd
import tempfile
import os
import time
from google.cloud import bigquery
from datetime import datetime, timedelta

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'xaxis-latam-psa-f3723b4290c7.json'

client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)

# Credential of the app_id, app_secret, access_token
app_id = '268153059269129'
app_secret = 'b6717d2ea80d2c631c916eb6b24bf353'
access_token = 'EAADz4jZCX2gkBO3GyuJQReGfHVE5gEp1tyPK33ZBU9NZCsGPUmYLoHeQqLBR9AvZCy5ardM40JWegqN0xbe9WQjK4pVuiZCedffLMGNqcicSgxgP87aYolawJ1dfOUEoDP93haIXZAWst0R77QsO5wvf49eAg3q9ntdUXZBhsWLHYyLqoXeWC0JV42k4C2o7VluGT3gQV3C'

# Initialize the FacebookAdsApi whit the credentials
FacebookAdsApi.init(app_id, app_secret, access_token)

my_acconunt = AdAccount('act_675908152846895')

# Funcionando obtener datos de mes para x campaña



date_now = datetime.now()

df_total = pd.DataFrame()


def date_90_days():
    date_90_days_ago = date_now - timedelta(days=90)
    formatted_day = date_90_days_ago.strftime('%Y-%m-%d')
    return formatted_day


def date_cycle(start_date, end_date):
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    df_final = pd.DataFrame()
    i = 0
    while i in range(current_date < end_date):
        params = {
            'time_range': {'since': current_date.strftime('%Y-%m-%d'), 'until': current_date.strftime('%Y-%m-%d')},
            'level': 'ad'
        }

        fields = [
            'ad_id',
            'adset_id',
            'clicks',
            'ctr',
            'cpc',
            'cost_per_unique_click',
            'frequency',
            'reach',
            'impressions',
            'spend',

        ]

        rawdata = my_acconunt.get_insights(
            params=params,
            fields=fields
        )
        rawdata = [x for x in rawdata]
        df_ciclo_actual = pd.DataFrame(rawdata)
        df_final = pd.concat([df_final, df_ciclo_actual], ignore_index=True)
        print(current_date)
        current_date += timedelta(days=1)
    return df_final


date = date_90_days()

df_delivery = date_cycle(date, date_now)

#Account ingishts get


with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tempfile:
    temp_path = tempfile.name
    print('Path del archivo: ', temp_path)
    df_delivery.to_csv(temp_path, index=False)

table_id = "xaxis-latam-psa.LATAM_Datawarehouse.Ad_delivery"

with open(temp_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

while job.state != 'DONE':
    time.sleep(2)
    job.reload()
    print(job.state)

print(job.result())

table = client.get_table(table_id)

print(
    "Loaded {} rows and {} columns to {}".format(table.num_rows,len(table.schema),table_id)
)
