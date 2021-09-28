from smartapi import SmartConnect
import pandas as pd  # for accessing the data and converting it in our format
from datetime import datetime, timedelta
import credentials  # for accessing id and password
import requests
from time import time, sleep
import threading
import warnings  # subclass of Exception
from talib import *

warnings.filterwarnings('ignore')

SYMBOL_LIST = ['ASIANTILES']  # Stock Name
TRADED_SYMBOL = []
timeFrame = 60 + 5  # 5 sec coz delay response of historical API


def place_order(token, symbol, qty, buy_sell, ordertype, price, variety='NORMAL', exch_seg='NSE', triggerprice=0):
    try:
        orderparams = {
            "variety": variety,
            "tradingsymbol": symbol,
            "symboltoken": token,
            "transactiontype": buy_sell,
            "exchange": exch_seg,
            "ordertype": ordertype,
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": price,
            "squareoff": "0",
            "stoploss": "0",
            "quantity": qty,
            "triggerprice": triggerprice
        }
        orderId = credentials.SMART_API_OBJ.placeOrder(orderparams)
        print("The order id is: {}".format(orderId))
    except Exception as e:
        print("Order placement failed: {}".format(e.message))


def intializeSymbolTokenMap():
    url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
    d = requests.get(url).json()
    global token_df
    token_df = pd.DataFrame.from_dict(d)
    token_df['expiry'] = pd.to_datetime(token_df['expiry'])
    token_df = token_df.astype({'strike': float})
    credentials.TOKEN_MAP = token_df


def getTokenInfo(symbol, exch_seg='NSE', instrumenttype='OPTIDX', strike_price='', pe_ce='CE'):
    df = credentials.TOKEN_MAP
    strike_price = strike_price * 100
    if exch_seg == 'NSE':  # for working in Equity ( In this project we are working on Equity only )
        eq_df = df[(df['exch_seg'] == 'NSE') & (df['symbol'].str.contains('EQ'))]
        return eq_df[eq_df['name'] == symbol]
    elif exch_seg == 'NFO' and ((instrumenttype == 'FUTSTK') or (instrumenttype == 'FUTIDX')):  # for working in Futures
        return df[
            (df['exch_seg'] == 'NFO') & (df['instrumenttype'] == instrumenttype) & (df['name'] == symbol)].sort_values(
            by=['expiry'])
    elif exch_seg == 'NFO' and (instrumenttype == 'OPTSTK' or instrumenttype == 'OPTIDX'):  # for working in Options
        return df[(df['exch_seg'] == 'NFO') & (df['instrumenttype'] == instrumenttype) & (df['name'] == symbol) & (
                    df['strike'] == strike_price) & (df['symbol'].str.endswith(pe_ce))].sort_values(by=['expiry'])


def calculate_indicator(res_json):
    columns = ['timestamp', 'O', 'H', 'L', 'C', 'V']
    df = pd.DataFrame(res_json['data'], columns=columns)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%dT%H:%M:%S')
    df['EMA'] = EMA(df.C, timeperiod=20)
    df['RSI'] = RSI(df.C, timeperiod=14)
    df['ATR'] = ATR(df.H, df.L, df.C, timeperiod=20)
    df['CROSS_UP'] = df['CROSS_DOWN'] = df['RSI_UP'] = 0
    df = df.round(decimals=2)

    for i in range(20, len(df)):
        if df['C'][i - 1] <= df['EMA'][i - 1] and df['C'][i] > df['EMA'][i]:  # if closing of candle happens above the EMA
            df['CROSS_UP'][i] = 1
        if df['C'][i - 1] >= df['EMA'][i - 1] and df['C'][i] < df['EMA'][i]:  # if closing of candle happens above the EMA
            df['CROSS_DOWN'][i] = 1
        if df['RSI'][i] > 50:  # if Relative Strength Index is above 50
            df['RSI_UP'][i] = 1

    print(df.tail(10))
    return df


def getHistoricalAPI(token, interval='ONE_MINUTE'):
    to_date = datetime.now()
    from_date = to_date - timedelta(days=5)
    from_date_format = from_date.strftime("%Y-%m-%d %H:%M")
    to_date_format = to_date.strftime("%Y-%m-%d %H:%M")
    try:
        historicParam = {
            "exchange": "NSE",
            "symboltoken": token,
            "interval": interval,
            "fromdate": from_date_format,
            "todate": to_date_format
        }
        candel_json = credentials.SMART_API_OBJ.getCandleData(historicParam)
        return calculate_indicator(candel_json)
    except Exception as e:
        print("Historic Api failed: {}".format(e.message))


def checkSignal():
    start = time()
    global TRADED_SYMBOL

    for symbol in SYMBOL_LIST:
        if symbol not in TRADED_SYMBOL:
            tokenInfo = getTokenInfo(symbol).iloc[0]
            token = tokenInfo['token']
            symbol = tokenInfo['symbol']
            print(symbol, token)
            candel_df = getHistoricalAPI(token)
            if candel_df is not None:
                latest_candel = candel_df.iloc[-1]
                if latest_candel['CROSS_UP'] == 1 and latest_candel['RSI_UP'] == 1:
                    ltp = latest_candel['C']
                    SL = ltp - 2 * latest_candel['ATR']
                    target = ltp + 5 * latest_candel['ATR']
                    qty = 1  # quantity to trade

                    res1 = place_order(token, symbol, qty, 'BUY', 'MARKET', 0)  # buy order
                    res2 = place_order(token, symbol, qty, 'SELL', 'STOPLOSS_MARKET', 0, variety='STOPLOSS',triggerprice=SL)  # SL order
                    res3 = place_order(token, symbol, qty, 'SELL', 'LIMIT', target)  # taget order
                    print(res1, res2, res3)
                    print(f'Order Placed for {symbol} SL {SL}  TGT {target} QTY {qty} at {datetime.now()}')
                    TRADED_SYMBOL.append(symbol)

    interval = timeFrame - (time() - start)
    print(interval)
    threading.Timer(interval, checkSignal).start()


if __name__ == '__main__':
    intializeSymbolTokenMap()
    obj = SmartConnect(api_key=credentials.API_KEY)
    data = obj.generateSession(credentials.USER_NAME, credentials.PWD)
    credentials.SMART_API_OBJ = obj

    interval = timeFrame - datetime.now().second
    print(f"Code run after {interval} sec")
    sleep(interval)
    checkSignal()
