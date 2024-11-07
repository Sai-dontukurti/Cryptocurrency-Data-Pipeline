#%%
# importing the requests library

import requests

# getting the url of our api

url = "https://api.coinpaprika.com/v1/tickers"

#response of the url

response = requests.get(url)

#lets write a function to check if the request is successful or not

if response.status_code == 200:
    data = response.json()
else:
    print("Response is not working due to status code:", response.status_code)


coins=[] # to store our coin data
for coin in data:
    coins.append({
        'name':coin['name'],
        'symbol':coin['symbol'],
        'price':coin['quotes']['USD']['price'],
        'updated_date':coin['last_updated'],
        'market_cap':coin['quotes']['USD']['market_cap'],
        'volume_24h':coin['quotes']['USD']['volume_24h'],
        'All_time_High':coin['quotes']['USD']['ath_price'],
        'All_time_high_date':coin['quotes']['USD']['ath_date'],
        'percent_change_24h':coin['quotes']['USD']['percent_change_24h']
    })


#%%

import pandas as pd
#storing into a data frame
df = pd.DataFrame(coins)
# %%
df.head()
# %%

#lets add the timestamp to the end of the data naming 
from datetime import datetime
time_stamp = datetime.now().strftime("%d-%m-%y")

#setting the folder path
import os
folder_path = 'Crypto/data/raw_data'
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

#%%
file_name = f'{folder_path}/crypto_bronze_data_{time_stamp}.csv'

df.to_csv(file_name,index=False)


# %%
