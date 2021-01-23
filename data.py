import requests
from datetime import datetime, timezone
import pandas as pd
import numpy as np

time_window = 21600  # 21600 seconds = 6h

pairs = ['btcusd', 'ethusd']


start_date = datetime(2017,10,1,0,0,tzinfo=timezone.utc).timestamp()  # 2017-8-18 is the first date bitstamp has ETH/USD data, for some reason
end_date = datetime(2021,1,1,0,0,tzinfo=timezone.utc).timestamp()

time_span = (end_date - start_date)  # total amount of seconds between the dates
total_candles = time_span/time_window  # each candle covers time_window seconds, so this is how many candles we have
total_requests_required = int(total_candles/1000) + 1  # each request can contain max 1000 data points


cols = ['timestamp']
cols.extend(pairs)  # .append method adds pairs as a list within the cols list

df = pd.DataFrame(columns=cols)

for i,j in zip(pairs, df.columns[1:]):  # i is for fetching data, j is for inserting into df

    timestamp = np.array([])
    closing_price = np.array([])

    step_size = time_window * 1000  # can only get 1000 obs per request, so need to break it up
    start = start_date
    #print('start timestamp: ' + str(start) + ' which is ' + str(datetime.utcfromtimestamp(start)))
    print(i)
    for k in range(total_requests_required):
        #print('From ' + str(datetime.utcfromtimestamp(start)) + ' to ' + str(datetime.utcfromtimestamp(start+step_size)))
        params = {'start': int(start), 'end': int(start+step_size), 'step': time_window, 'limit': 1000}
        r = requests.get('https://www.bitstamp.net/api/v2/ohlc/' + i , params=params)
        r_dict = r.json()

        for q in range(len(r_dict['data']['ohlc'])):
            timestamp = np.append(timestamp, float(r_dict['data']['ohlc'][q]['timestamp']))
            closing_price = np.append(closing_price, float(r_dict['data']['ohlc'][q]['close']))

        start = start + step_size

    if i == 'btcusd':
        timestamp_copy = timestamp


    print(len(timestamp))
    print(len(closing_price))
    # df[j] = closing_price

#print(np.setdiff1d(timestamp, timestamp_copy))
diff = np.setdiff1d(timestamp, timestamp_copy)
print(str(datetime.utcfromtimestamp(diff)))
print('')



# params = {'start': int(diff), 'end': int(diff), 'step': time_window, 'limit': 4}
# r = requests.get('https://www.bitstamp.net/api/v2/ohlc/btcusd', params=params)
# r_dict = r.json()
# print(str(datetime.utcfromtimestamp(int(r_dict['data']['ohlc'][0]['timestamp']))))
# print(str(datetime.utcfromtimestamp(int(r_dict['data']['ohlc'][1]['timestamp']))))
# print(str(datetime.utcfromtimestamp(int(r_dict['data']['ohlc'][2]['timestamp']))))
# print(str(datetime.utcfromtimestamp(int(r_dict['data']['ohlc'][3]['timestamp']))))
# print('')
# print(str(datetime.utcfromtimestamp(diff[0])))











