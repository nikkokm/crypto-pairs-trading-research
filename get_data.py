import requests
from datetime import datetime

start = datetime(2016,1,1).timestamp()
end = datetime(2016,1,4).timestamp()

params = {'start':int(start), 'end':int(end), 'step':21600, 'limit':1000}
r = requests.get('https://www.bitstamp.net/api/v2/ohlc/ethusd', params=params)
r_dict = r.json()

print(r_dict['data']['ohlc'])