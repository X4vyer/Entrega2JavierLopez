import requests
import os
import sqlalchemy as sa
import pandas as pd
from datetime import datetime

url = "http://dataservice.accuweather.com/forecasts/v1/daily/5day/349727?apikey=ndEZ2Nv47o8FS16m8lIpR2KThVsPCH9w"
response = requests.get(url)

data = response.json()
daily_forecasts = data.get('DailyForecasts', [])

dates = []
min_temps = []
max_temps = []
narrative = []

fecha_carga = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

for forecast in daily_forecasts:
    dates.append(forecast.get('Date'))
    min_temps.append(forecast.get('Temperature', {}).get('Minimum', {}).get('Value'))
    max_temps.append(forecast.get('Temperature', {}).get('Maximum', {}).get('Value'))
    narrative.append(forecast.get('Day', {}).get('IconPhrase'))

df = pd.DataFrame({
    'Date': dates,
    'temperaturaminima': min_temps,
    'temperaturamaxima': max_temps,
    'descripcion': narrative,
    'FechadeCarga': fecha_carga  
})

connection_url = f"redshift+redshift_connector://javierlocar05_coderhouse:H2wt20L9MF@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database"
db_engine = sa.create_engine(connection_url)
df.to_sql("api_tiempo",db_engine,if_exists = 'replace',schema = 'javierlocar05_coderhouse',  index = False)