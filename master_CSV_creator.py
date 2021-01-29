#####

#df live data to update historical
import cryptocompare
import pandas as pd
import os
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
import numpy as np
from datetime import datetime as dt

#btc hourly
a = cryptocompare.get_historical_price_hour('BTC','usd')

#eth hourly
b = cryptocompare.get_historical_price_hour('ETH','usd')

#ada hourly
c = cryptocompare.get_historical_price_hour('ADA','usd')

#hourly df
    #btc
df1 = pd.DataFrame(a)
df1['currency'] = 'btc'
df1['price_USD'] = df1['close']
    #eth
df2 = pd.DataFrame(b)
df2['currency'] = 'eth'
df2['price_USD'] = df2['close']
    #ada
df3 = pd.DataFrame(c)
df3['currency'] = 'ada'

#hourly df touched up
df1a = df1[['currency', 'time', 'price_USD']]
df2a = df2[['currency', 'time', 'price_USD']]

#convert price ada
df3a = pd.merge(df3, df1a, on='time', how='left')
df3a['price1'] = df3a['close'] * df3a['price_USD']
df3b = df3a[['currency_x', 'time', 'price1']]

#align ada columns
df3c = df3b.rename(columns = {'price1':'price_USD', 'currency_x':'currency'})
df3d = df3c[['currency', 'time', 'price_USD']]

#define df, merge btc, eth, ada
frames = [df1a, df2a, df3d]

#df live data
df_live = pd.concat(frames)
df_live['key'] = df_live['time'].astype(str) + df_live['currency']
df_live = df_live.rename(columns = {'price':'price_usd', 'time':'timestamp_UTC'})
df_live['datetime_UTC'] = pd.to_datetime(df_live['timestamp_UTC'], unit='s')

df_live = df_live[['currency', 'datetime_UTC', 'timestamp_UTC', 'price_USD', 'key']]

df_live = df_live.loc[(df_live['timestamp_UTC'] > 1584129600)]

df_live_btc = df_live.loc[(df_live['currency'] == 'btc')]
df_live_eth = df_live.loc[(df_live['currency'] == 'eth')]
df_live_ada = df_live.loc[(df_live['currency'] == 'ada')]

######


#df historical data txt files cleaned up and framed

#btc
df4 = pd.read_csv('btc.txt', sep=",", header=None)
df4 = df4.rename(columns = {0:'date',1:'coin', 2:'open', 3:'high', 4:'low', 5: 'close', 6:'volume coin', 7:'volume USD'})
df4 = pd.DataFrame(df4)
df4['currency'] = 'btc'
df4['datetime_UTC'] = pd.to_datetime(df4['date'], format="%Y-%m-%d %I-%p")
df4['timestamp_UTC'] = df4.datetime_UTC.values.astype(np.int64) // 10 ** 9

#eth
df6 = pd.read_csv('eth.txt', sep=",", header=None)
df6 = df6.rename(columns = {0:'date',1:'coin', 2:'open', 3:'high', 4:'low', 5: 'price_USD', 6:'volume coin', 7:'volume USD'})
df6 = pd.DataFrame(df6)
df6['currency'] = 'eth'
df6['datetime_UTC'] = pd.to_datetime(df6['date'], format="%Y-%m-%d %I-%p")
df6['timestamp_UTC'] = df6.datetime_UTC.values.astype(np.int64) // 10 ** 9

#ada
df5 = pd.read_csv('ada.txt', sep=",", header=None)
df5 = df5.rename(columns = {0:'date',1:'coin', 2:'open', 3:'high', 4:'low', 5: 'close', 6:'volume coin', 7:'volume BTC'})
df5 = pd.DataFrame(df5)
df5['currency'] = 'ada'
df5 = pd.merge(df5, df4, on='date', how='left')
df5['price_USD'] = df5['close_x'] * df5['close_y']
df5['datetime_UTC'] = pd.to_datetime(df5['date'], format="%Y-%m-%d %I-%p")
df5['timestamp_UTC'] = df5.datetime_UTC.values.astype(np.int64) // 10 ** 9
df5 = df5.rename(columns = {'currency_x':'currency'})

#df historical
df4['price_USD'] = df4['close']
df4a = df4[['currency', 'datetime_UTC', 'timestamp_UTC', 'price_USD']]
df5a = df5[['currency', 'datetime_UTC', 'timestamp_UTC', 'price_USD']]
df6a = df6[['currency', 'datetime_UTC', 'timestamp_UTC', 'price_USD']]

frames1 = [df4a, df5a, df6a]

df_historical_btc = df4a
df_historical_eth = df6a
df_historical_ada = df5a

df_historical_btc['key'] = df_historical_btc['timestamp_UTC'].astype(str) + df_historical_btc['currency']
df_historical_eth['key'] = df_historical_eth['timestamp_UTC'].astype(str) + df_historical_eth['currency']
df_historical_ada['key'] = df_historical_ada['timestamp_UTC'].astype(str) + df_historical_ada['currency']

#csv btc

df_csv = pd.read_csv('df_btc_master.csv')
df_csv = df_csv.rename(columns={0: 'currency', 1: 'datetime_UTC_buy', 2: 'timestamp_UTC_buy', 3: 'price_USD_buy', 4: 'key'})
df_csv = pd.concat([df_live_btc, df_csv])
df_csv = df_csv.drop_duplicates(subset=df_csv.columns.difference(['key']))
os.remove('df_btc_master.csv')
df_csv.to_csv('df_btc_master.csv', index=False)
df_csv = pd.read_csv('df_btc_master.csv')
df_csv = df_csv.rename(columns={0: 'currency', 1: 'datetime_UTC_buy', 2: 'timestamp_UTC_buy', 3: 'price_USD_buy', 4: 'key'})
df_csv = df_csv.drop_duplicates(subset=df_csv.columns.difference(['key']))
df_csv.to_csv('df_btc_master.csv', index=False)
df_historical_btc = pd.read_csv('df_btc_master.csv')
df_historical_btc = df_historical_btc.rename(columns={0: 'currency', 1: 'datetime_UTC_buy', 2: 'timestamp_UTC_buy', 3: 'price_USD_buy', 4: 'key'})
df_historical_btc = df_historical_btc.drop_duplicates(subset=df_historical_btc.columns.difference(['key']))
df_historical_btc = df_historical_btc.sort_values(by='timestamp_UTC')

#csv eth
df_csv = pd.read_csv('df_eth_master.csv')
df_csv = df_csv.rename(columns={0: 'currency', 1: 'datetime_UTC_buy', 2: 'timestamp_UTC_buy', 3: 'price_USD_buy', 4: 'key'})
df_csv = pd.concat([df_live_eth, df_csv])
df_csv = df_csv.drop_duplicates(subset=df_csv.columns.difference(['key']))
os.remove('df_eth_master.csv')
df_csv.to_csv('df_eth_master.csv', index=False)
df_csv = pd.read_csv('df_eth_master.csv')
df_csv = df_csv.rename(columns={0: 'currency', 1: 'datetime_UTC_buy', 2: 'timestamp_UTC_buy', 3: 'price_USD_buy', 4: 'key'})
df_csv = df_csv.drop_duplicates(subset=df_csv.columns.difference(['key']))
df_csv.to_csv('df_eth_master.csv', index=False)
df_historical_eth = pd.read_csv('df_eth_master.csv')
df_historical_eth = df_historical_eth.rename(columns={0: 'currency', 1: 'datetime_UTC_buy', 2: 'timestamp_UTC_buy', 3: 'price_USD_buy', 4: 'key'})
df_historical_eth = df_historical_eth.drop_duplicates(subset=df_historical_eth.columns.difference(['key']))
df_historical_eth = df_historical_eth.sort_values(by='timestamp_UTC')

#csv ada
df_csv = pd.read_csv('df_ada_master.csv')
df_csv = df_csv.rename(columns={0: 'currency', 1: 'datetime_UTC_buy', 2: 'timestamp_UTC_buy', 3: 'price_USD_buy', 4: 'key'})
df_csv = pd.concat([df_live_ada, df_csv])
df_csv = df_csv.drop_duplicates(subset=df_csv.columns.difference(['key']))
os.remove('df_ada_master.csv')
df_csv.to_csv('df_ada_master.csv', index=False)
df_csv = pd.read_csv('df_ada_master.csv')
df_csv = df_csv.rename(columns={0: 'currency', 1: 'datetime_UTC_buy', 2: 'timestamp_UTC_buy', 3: 'price_USD_buy', 4: 'key'})
df_csv = df_csv.drop_duplicates(subset=df_csv.columns.difference(['key']))
df_csv.to_csv('df_ada_master.csv', index=False)
df_historical_ada = pd.read_csv('df_ada_master.csv')
df_historical_ada = df_historical_ada.rename(columns={0: 'currency', 1: 'datetime_UTC_buy', 2: 'timestamp_UTC_buy', 3: 'price_USD_buy', 4: 'key'})
df_historical_ada = df_historical_ada.drop_duplicates(subset=df_historical_ada.columns.difference(['key']))
df_historical_ada = df_historical_ada.sort_values(by='timestamp_UTC')

long_ema = 26
short_ema = 12
signal = 9

df_historical_btc['long ema'] = df_historical_btc['price_USD'].ewm(span=long_ema,min_periods=0,adjust=False,ignore_na=False).mean()
df_historical_btc['short ema'] = df_historical_btc['price_USD'].ewm(span=short_ema,min_periods=0,adjust=False,ignore_na=False).mean()
df_historical_btc['MACD'] = (df_historical_btc['short ema'] - df_historical_btc['long ema'])
df_historical_btc['signal'] = df_historical_btc['MACD'].ewm(span=signal,min_periods=0,adjust=False,ignore_na=False).mean()
df_historical_btc['buy'] = (df_historical_btc['MACD'].shift(1) < df_historical_btc['signal'].shift(1)) & (df_historical_btc['MACD'] >= df_historical_btc['signal'])
df_historical_btc['sell'] = (df_historical_btc['MACD'].shift(1) > df_historical_btc['signal'].shift(1)) & (df_historical_btc['MACD'] <= df_historical_btc['signal'])

df_historical_btc = df_historical_btc.tail((len(df_historical_btc.index)) - long_ema)

df_crossover_btc = df_historical_btc.loc[(df_historical_btc['buy'] == True) | (df_historical_btc['sell'] == True)]

df_crossover_btc = df_crossover_btc.sort_values(by='timestamp_UTC')

df_crossover_btc_row1 = df_crossover_btc.head(1)
df_crossover_btc_row1 = df_crossover_btc_row1[(df_crossover_btc_row1['buy'] == True)]
df_crossover_btc_row2 = df_crossover_btc.tail(1)
df_crossover_btc_row2 = df_crossover_btc_row2[(df_crossover_btc_row2['sell'] == True)]
df_crossover_btc_analysis = df_crossover_btc
df_crossover_btc_analysis = df_crossover_btc_analysis.drop(df_crossover_btc_analysis.index[0])
df_crossover_btc_analysis = df_crossover_btc_analysis.drop(df_crossover_btc_analysis.index[-1])
df_crossover_btc_analysis = pd.concat([df_crossover_btc_row1, df_crossover_btc_row2, df_crossover_btc_analysis])
df_crossover_btc_analysis = df_crossover_btc_analysis.sort_values(by='timestamp_UTC')

df_crossover_btc_analysis['long_ema_var'] = long_ema
df_crossover_btc_analysis['short_ema_var'] = short_ema
df_crossover_btc_analysis['signal_var'] = signal

df_crossover_btc_analysis['analysis_key'] = df_crossover_btc_analysis['long_ema_var'].map(str) + '-' + df_crossover_btc_analysis['short_ema_var'].map(str) + '-' + df_crossover_btc_analysis['signal_var'].map(str) + df_crossover_btc_analysis['currency']

df_crossover_btc_analysis_buy = df_crossover_btc_analysis.loc[(df_crossover_btc_analysis['buy'] == True)]

df_crossover_btc_analysis_buy['buy_sell_key'] = np.arange(len(df_crossover_btc_analysis_buy))

df_crossover_btc_analysis_sell = df_crossover_btc_analysis.loc[(df_crossover_btc_analysis['sell'] == True)]

df_crossover_btc_analysis_sell['buy_sell_key'] = np.arange(len(df_crossover_btc_analysis_sell))

df_crossover_btc_analysis_buy_and_sell = df_crossover_btc_analysis

df_crossover_btc_analysis_data = pd.merge(df_crossover_btc_analysis_buy, df_crossover_btc_analysis_sell, on='buy_sell_key', how='left')

df_crossover_btc_analysis = df_crossover_btc_analysis_data[['currency_x', 'datetime_UTC_x', 'timestamp_UTC_x', 'price_USD_x', 'key_x', 'analysis_key_x', 'datetime_UTC_y', 'timestamp_UTC_y', 'price_USD_y', 'key_y']]

df_crossover_btc_analysis = df_crossover_btc_analysis.rename(columns = {'currency_x':'currency', 'datetime_UTC_x':'datetime_UTC_buy', 'timestamp_UTC_x':'timestamp_UTC_buy', 'price_USD_x':'price_USD_buy', 'key_x':'data_key_buy', 'analysis_key_x': 'analysis_key', 'datetime_UTC_y':'datetime_UTC_sell', 'timestamp_UTC_y':'timestamp_UTC_sell', 'price_USD_y':'price_USD_sell', 'key_y':'data_key_sell'})

df_crossover_btc_analysis['profit'] = df_crossover_btc_analysis['price_USD_sell'] - df_crossover_btc_analysis['price_USD_buy']

##### buy sell

df_historical_eth['long ema'] = df_historical_eth['price_USD'].ewm(span=long_ema,min_periods=0,adjust=False,ignore_na=False).mean()
df_historical_eth['short ema'] = df_historical_eth['price_USD'].ewm(span=short_ema,min_periods=0,adjust=False,ignore_na=False).mean()
df_historical_eth['MACD'] = (df_historical_eth['short ema'] - df_historical_eth['long ema'])
df_historical_eth['signal'] = df_historical_eth['MACD'].ewm(span=signal,min_periods=0,adjust=False,ignore_na=False).mean()
df_historical_eth['buy'] = (df_historical_eth['MACD'].shift(1) < df_historical_eth['signal'].shift(1)) & (df_historical_eth['MACD'] >= df_historical_eth['signal'])
df_historical_eth['sell'] = (df_historical_eth['MACD'].shift(1) > df_historical_eth['signal'].shift(1)) & (df_historical_eth['MACD'] <= df_historical_eth['signal'])

df_historical_eth = df_historical_eth.tail((len(df_historical_eth.index)) - long_ema)

df_crossover_eth = df_historical_eth.loc[(df_historical_eth['buy'] == True) | (df_historical_eth['sell'] == True)]

df_crossover_eth = df_crossover_eth.sort_values(by='timestamp_UTC')

df_crossover_eth_row1 = df_crossover_eth.head(1)
df_crossover_eth_row1 = df_crossover_eth_row1[(df_crossover_eth_row1['buy'] == True)]
df_crossover_eth_row2 = df_crossover_eth.tail(1)
df_crossover_eth_row2 = df_crossover_eth_row2[(df_crossover_eth_row2['sell'] == True)]
df_crossover_eth_analysis = df_crossover_eth
df_crossover_eth_analysis = df_crossover_eth_analysis.drop(df_crossover_eth_analysis.index[0])
df_crossover_eth_analysis = df_crossover_eth_analysis.drop(df_crossover_eth_analysis.index[-1])
df_crossover_eth_analysis = pd.concat([df_crossover_eth_row1, df_crossover_eth_row2, df_crossover_eth_analysis])
df_crossover_eth_analysis = df_crossover_eth_analysis.sort_values(by='timestamp_UTC')

df_crossover_eth_analysis['long_ema_var'] = long_ema
df_crossover_eth_analysis['short_ema_var'] = short_ema
df_crossover_eth_analysis['signal_var'] = signal

df_crossover_eth_analysis['analysis_key'] = df_crossover_eth_analysis['long_ema_var'].map(str) + '-' + df_crossover_eth_analysis['short_ema_var'].map(str) + '-' + df_crossover_eth_analysis['signal_var'].map(str) + df_crossover_eth_analysis['currency']

df_crossover_eth_analysis_buy = df_crossover_eth_analysis.loc[(df_crossover_eth_analysis['buy'] == True)]

df_crossover_eth_analysis_buy['buy_sell_key'] = np.arange(len(df_crossover_eth_analysis_buy))

df_crossover_eth_analysis_sell = df_crossover_eth_analysis.loc[(df_crossover_eth_analysis['sell'] == True)]

df_crossover_eth_analysis_sell['buy_sell_key'] = np.arange(len(df_crossover_eth_analysis_sell))

df_crossover_eth_analysis_buy_and_sell = df_crossover_eth_analysis

df_crossover_eth_analysis_data = pd.merge(df_crossover_eth_analysis_buy, df_crossover_eth_analysis_sell, on='buy_sell_key', how='left')

df_crossover_eth_analysis = df_crossover_eth_analysis_data[['currency_x', 'datetime_UTC_x', 'timestamp_UTC_x', 'price_USD_x', 'key_x', 'analysis_key_x', 'datetime_UTC_y', 'timestamp_UTC_y', 'price_USD_y', 'key_y']]

df_crossover_eth_analysis = df_crossover_eth_analysis.rename(columns = {'currency_x':'currency', 'datetime_UTC_x':'datetime_UTC_buy', 'timestamp_UTC_x':'timestamp_UTC_buy', 'price_USD_x':'price_USD_buy', 'key_x':'data_key_buy', 'analysis_key_x': 'analysis_key', 'datetime_UTC_y':'datetime_UTC_sell', 'timestamp_UTC_y':'timestamp_UTC_sell', 'price_USD_y':'price_USD_sell', 'key_y':'data_key_sell'})

df_crossover_eth_analysis['profit'] = df_crossover_eth_analysis['price_USD_sell'] - df_crossover_eth_analysis['price_USD_buy']

#####ada

df_historical_ada['long ema'] = df_historical_ada['price_USD'].ewm(span=long_ema,min_periods=0,adjust=False,ignore_na=False).mean()
df_historical_ada['short ema'] = df_historical_ada['price_USD'].ewm(span=short_ema,min_periods=0,adjust=False,ignore_na=False).mean()
df_historical_ada['MACD'] = (df_historical_ada['short ema'] - df_historical_ada['long ema'])
df_historical_ada['signal'] = df_historical_ada['MACD'].ewm(span=signal,min_periods=0,adjust=False,ignore_na=False).mean()
df_historical_ada['buy'] = (df_historical_ada['MACD'].shift(1) < df_historical_ada['signal'].shift(1)) & (df_historical_ada['MACD'] >= df_historical_ada['signal'])
df_historical_ada['sell'] = (df_historical_ada['MACD'].shift(1) > df_historical_ada['signal'].shift(1)) & (df_historical_ada['MACD'] <= df_historical_ada['signal'])

df_historical_ada = df_historical_ada.tail((len(df_historical_ada.index)) - long_ema)

df_crossover_ada = df_historical_ada.loc[(df_historical_ada['buy'] == True) | (df_historical_ada['sell'] == True)]

df_crossover_ada = df_crossover_ada.sort_values(by='timestamp_UTC')

df_crossover_ada_row1 = df_crossover_ada.head(1)
df_crossover_ada_row1 = df_crossover_ada_row1[(df_crossover_ada_row1['buy'] == True)]
df_crossover_ada_row2 = df_crossover_ada.tail(1)
df_crossover_ada_row2 = df_crossover_ada_row2[(df_crossover_ada_row2['sell'] == True)]
df_crossover_ada_analysis = df_crossover_ada
df_crossover_ada_analysis = df_crossover_ada_analysis.drop(df_crossover_ada_analysis.index[0])
df_crossover_ada_analysis = df_crossover_ada_analysis.drop(df_crossover_ada_analysis.index[-1])
df_crossover_ada_analysis = pd.concat([df_crossover_ada_row1, df_crossover_ada_row2, df_crossover_ada_analysis])
df_crossover_ada_analysis = df_crossover_ada_analysis.sort_values(by='timestamp_UTC')

df_crossover_ada_analysis['long_ema_var'] = long_ema
df_crossover_ada_analysis['short_ema_var'] = short_ema
df_crossover_ada_analysis['signal_var'] = signal

df_crossover_ada_analysis['analysis_key'] = df_crossover_ada_analysis['long_ema_var'].map(str) + '-' + df_crossover_ada_analysis['short_ema_var'].map(str) + '-' + df_crossover_ada_analysis['signal_var'].map(str) + df_crossover_ada_analysis['currency']

df_crossover_ada_analysis_buy = df_crossover_ada_analysis.loc[(df_crossover_ada_analysis['buy'] == True)]

df_crossover_ada_analysis_buy['buy_sell_key'] = np.arange(len(df_crossover_ada_analysis_buy))

df_crossover_ada_analysis_sell = df_crossover_ada_analysis.loc[(df_crossover_ada_analysis['sell'] == True)]

df_crossover_ada_analysis_sell['buy_sell_key'] = np.arange(len(df_crossover_ada_analysis_sell))

df_crossover_ada_analysis_buy_and_sell = df_crossover_ada_analysis

df_crossover_ada_analysis_data = pd.merge(df_crossover_ada_analysis_buy, df_crossover_ada_analysis_sell, on='buy_sell_key', how='left')

df_crossover_ada_analysis = df_crossover_ada_analysis_data[['currency_x', 'datetime_UTC_x', 'timestamp_UTC_x', 'price_USD_x', 'key_x', 'analysis_key_x', 'datetime_UTC_y', 'timestamp_UTC_y', 'price_USD_y', 'key_y']]

df_crossover_ada_analysis = df_crossover_ada_analysis.rename(columns = {'currency_x':'currency', 'datetime_UTC_x':'datetime_UTC_buy', 'timestamp_UTC_x':'timestamp_UTC_buy', 'price_USD_x':'price_USD_buy', 'key_x':'data_key_buy', 'analysis_key_x': 'analysis_key', 'datetime_UTC_y':'datetime_UTC_sell', 'timestamp_UTC_y':'timestamp_UTC_sell', 'price_USD_y':'price_USD_sell', 'key_y':'data_key_sell'})

df_crossover_ada_analysis['profit'] = df_crossover_ada_analysis['price_USD_sell'] - df_crossover_ada_analysis['price_USD_buy']

####

df_analysis_master_data = pd.concat([df_crossover_btc_analysis, df_crossover_eth_analysis, df_crossover_ada_analysis])

df_analysis_master_data['analysis_key_detail'] = df_analysis_master_data['analysis_key'].map(str) + '-' + df_analysis_master_data['timestamp_UTC_buy'].map(str) + '-' + df_analysis_master_data['timestamp_UTC_sell'].map(str)

df_analysis_master_data.to_csv('df_analysis_master_data.csv', index=False)



