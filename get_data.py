import requests
from datetime import datetime
import pandas as pd
import numpy as np

time_window = 21600  # 21600 seconds = 6h

pairs = ['btcusd', 'ethusd']

start_date = datetime(2016,1,1).timestamp()
end_date = datetime(2021,1,1).timestamp()

time_span = (end_date - start_date)  # total amount of seconds between the dates
print(time_span)
total_candles = time_span/time_window  # each candle covers time_window seconds, so this is how many candles we have
total_requests_required = int(total_candles/1000) + 1  # each request can contain max 1000 data poiunts

timestamp = np.array([])
btcusd = np.array([])
ethusd = np.array([])


step_size = time_window * 1000  # can only get 1000 obs per request, so need to break it up
start = start_date
for j in range(total_requests_required):
    params = {'start':int(start), 'end':int(start+step_size), 'step':time_window, 'limit':1000}
    r = requests.get('https://www.bitstamp.net/api/v2/ohlc/btcusd', params=params)
    r_dict = r.json()

    for i in range(len(r_dict['data']['ohlc'])):
        timestamp = np.append(timestamp, float(r_dict['data']['ohlc'][i]['timestamp']))
        btcusd = np.append(btcusd, float(r_dict['data']['ohlc'][i]['close']))

    start = start + step_size


datetime_array = np.array([])
for i in range(len(timestamp)):
    datetime_array = np.append(datetime_array, datetime.fromtimestamp(timestamp[i]))




df = pd.DataFrame(
    {'date':datetime_array,
     'btcusd':btcusd}
)

df.drop_duplicates(inplace=True)
print(df)



