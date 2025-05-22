import pandas as pd
from conf.config import *
import requests






def download_data():



    base_url = 'https://api.open-meteo.com/v1/forecast?'
    url_params = 'latitude=52.52&longitude=13.41&hourly=temperature_2m,wind_speed_10m&models=*&timezone=Australia%2FBrisbane&forecast_days=10'
    req_url = base_url + url_params
    payload = pd.DataFrame(requests.get(req_url).json())

    variables = [c for c in payload.index if c!='time']
    current_time = pd.Timestamp.today()
    holddf = []
    time_idx = pd.to_datetime(payload.loc['time','hourly'])
    for v in variables:
        tmprow = payload.loc[v]
        tmpobs = tmprow.loc['hourly']
        tmpdf = pd.DataFrame(tmpobs, index=time_idx, columns=[v])
        tmpdf = tmpdf.loc[:tmpdf.last_valid_index()]
        tmpdf['units'] = tmprow['hourly_units']
        tmpdf['latitude'] = tmprow['latitude']
        tmpdf['longitude'] = tmprow['longitude']
        tmpdf['elevation'] = tmprow['elevation']
        tmpdf['timezone'] = tmprow['timezone']
        tmpdf['timezone_abbreviation'] = tmprow['timezone_abbreviation']
        tmpdf['collect_datetime'] = current_time
        holddf.append(tmpdf)
    return df


if __name__ == '__main__':
    df = download_data()
