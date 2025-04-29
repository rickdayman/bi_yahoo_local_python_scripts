import os
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd
import numpy as np
import yfinance as yf
import talib as ta
import urllib
import pyodbc
from sqlalchemy import create_engine

run_start = time.time()
print_time = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
print(print_time + ': starting')

###############################################################################################################
# set up environment and start spark session
###############################################################################################################

# # could also add to environmental variables (system)
# os.environ['PYSPARK_PYTHON'] = "C:\\Users\\rickd\\AppData\\Local\\Programs\\Python\\Launcher\\py.exe"
# os.environ['PYSPARK_DRIVER_PYTHON'] = "C:\\Users\\rickd\\AppData\\Local\\Programs\\Python\\Launcher\\py.exe"

# # could also add to this folder C:\Program Files\spark\jars (which i've done for the sql.write)
# jdbcdriverpath = 'C:\\Program Files\\Java\\sqljdbc_12.8\\enu\\jars\\mssql-jdbc-12.8.1.jre8.jar'

# spark = SparkSession.builder \
#                             .master('local') \
#                             .appName('pyspark playing') \
#                             .config("spark.driver.extraClassPath", jdbcdriverpath) \
#                             .getOrCreate()


# https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-azurefunctions

#################################################################################################
# get start date from vw_ohlc_incremental_date
#################################################################################################

quoted = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=RICKVICTUS;DATABASE=YAHOO;Trusted_Connection=yes;")
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted), fast_executemany=True)

df_inc_date = pd.read_sql_query("SELECT incremental_date FROM dbo.vw_ohlc_incremental_date", con=engine)
startdate = df_inc_date.at[0, 'incremental_date']

#################################################################################################
# get list of tickers
#################################################################################################

#https://www.cboe.com/us/equities/market_statistics/listed_symbols/

#startdate = datetime(2024, 12, 31, 0, 0)
ta_startdate = startdate + timedelta(days=-49)

fx_gold_oil = ['GC=F','GBPUSD=X','EURUSD=X','USDJPY=X', 'CL=F', '^GSPC', 'BTC-USD']

#symbolslist = pd.read_csv("C:\\Users\\rickd\\Documents\\ACER\\ACER\\Sequelytics\\CV\\WorkSamples\\Python_Training\\YAHOO\\stock_tickers.csv")
symbolslist = pd.read_csv("C:\\Users\\rickd\\Documents\\ACER\\ACER\\Sequelytics\\CV\\WorkSamples\\Python_Training\\YAHOO\\stock_tickers_top_10.csv")

symbols = symbolslist['symbol'].to_list()

# print(symbols)
symbols = ['AAPL', 'BRK-B', 'TSLA', 'NVDA', 'RKLB', 'META', 'MSTR', 'AVGO', 'JPM', 'AMZN']

symbols = symbols + fx_gold_oil

#################################################################################################
# get daily ohlc data from yahoo
#################################################################################################

df_data = pd.DataFrame(yf.download(symbols, start = ta_startdate, threads = True, proxy = None, group_by = 'ticker', auto_adjust=False))

df_data = df_data.stack(level=0, future_stack = True).rename_axis(['Date', 'Ticker']).reset_index()
df_data['Date'] = pd.to_datetime(df_data['Date'], format = '%Y-%m-%d').dt.tz_localize(None)
df_data.columns = [col.lower() for col in df_data]
df_data = df_data.sort_values(['ticker', 'date'], ascending = [True, True])

df_data.dropna(how='any', inplace=True)

del df_data['adj close']
del df_data['volume']

df_data['previous_close'] = df_data['close'].shift(1, fill_value=0)
df_data['previous_close'] = np.where((df_data['previous_close'] == 0), df_data['close'], df_data['previous_close'])
df_data['percent_increase'] = df_data.apply(lambda x: (x.close - x.previous_close) / x.previous_close, axis=1)
df_data['percent_increase_multipler'] = df_data.apply(lambda x: 1 + ((x.close - x.previous_close) / x.previous_close), axis=1)
df_data['rsi_close'] = ta.RSI(df_data['close'], timeperiod = 14)
df_data['atr_close'] = ta.ATR(df_data['high'], df_data['low'], df_data['close'], timeperiod = 14)
df_data['adx_close'] = ta.ADX(df_data['high'], df_data['low'], df_data['close'], timeperiod = 14)

df_data = df_data[df_data.date > startdate]
#df_data.reset_index()

#print(df_data)

print_time = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
print(print_time + ': transform ohlc data complete')

#################################################################################################
## Load data from yahoo into dataframe
#################################################################################################

date_recent = datetime.now() + timedelta(days=-28)
df_recent_tickers = df_data[df_data.date >= date_recent]
symbols = df_recent_tickers.ticker.unique()

allholders = pd.DataFrame()
allinfo = pd.DataFrame()
symbols = [x for x in symbols if x not in fx_gold_oil]
symbol_count = len(symbols)
symbol_counter = 0

for symbol in symbols:
    try:
        stock = yf.Ticker(symbol)

        holders = pd.DataFrame(stock.institutional_holders)
        holders['symbol'] = symbol

        info = pd.DataFrame(index = stock.info, data = stock.info.values(), columns = [stock.info['symbol']])
        info = info.T

        allholders = pd.concat([allholders, holders])
        allinfo = pd.concat([allinfo, info])

    except:
        print("Error on ticker - " + symbol)
        continue

    symbol_counter = symbol_counter + 1
    percent_bucket = int(symbol_count / 10)
    if(percent_bucket == 0):
        percent_bucket = 1

    if(symbol_counter % percent_bucket == 0):
        print_time = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
        print(print_time + ': ' + str(int(symbol_counter / percent_bucket * 10)) + ' % complete')

allinfo = allinfo.reset_index()

print_time = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
print(print_time + ': all stock/investor info completed')

#################################################################################################
## Add data to SQL server
#################################################################################################

# quoted = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=RICKVICTUS;DATABASE=YAHOO;Trusted_Connection=yes;")
# engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted), fast_executemany=True)

allholders.to_sql("yfinance_institutional_investors", schema="stg", con=engine, index=True, if_exists='replace', method='multi', chunksize=250)
allinfo.to_sql("yfinance_stock_information", schema="stg", con=engine, index=True, if_exists='replace', method='multi', chunksize=10)

print_time = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
print(print_time + ': stock/investor info loaded to sql db completed')

#################################################################################################
# Export data to csv and then bulk insert into SQL because performs better than to_sql over
# large datasets
#################################################################################################

# creates table structure if dataframe changes
#df_data.to_sql("yfinance_daily_stock_prices", schema="stg", con=engine, index=False, if_exists='replace', method='multi', chunksize=200) 

dailystockpricesfilename = "C:\\Users\\rickd\\Documents\\ACER\\ACER\\Sequelytics\\CV\\WorkSamples\\Python_Training\\YAHOO\\dailystockprices.csv"

# If file exists, delete it.
if os.path.isfile(dailystockpricesfilename):
    os.remove(dailystockpricesfilename)
else:
    # If it fails, inform the user.
    print("Error: %s file not found" % dailystockpricesfilename)

df_data.to_csv(dailystockpricesfilename, encoding = 'utf-8', index = False)

cnxn = pyodbc.connect('Trusted_Connection=yes', driver='{SQL Server}', server='RICKVICTUS', database='YAHOO')
cursor = cnxn.cursor()

cursor.execute("TRUNCATE TABLE stg.yfinance_daily_stock_prices")

importcsvsql = """
BULK INSERT stg.yfinance_daily_stock_prices
FROM '""" + dailystockpricesfilename + """' WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR=',',
    ROWTERMINATOR='\\n'
    );
"""

cursor.execute(importcsvsql)
cnxn.commit()
cursor.close()
cnxn.close()

#################################################################################################
# print run times to terminal
#################################################################################################

print_time = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
print(print_time + ': daily price data loaded to sql db completed')

run_end = time.time()
print(run_end - run_start)

