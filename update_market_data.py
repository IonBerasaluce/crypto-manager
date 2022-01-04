import ccxt
from global_vars import *
from tools import stringToTimeStamp, timestampToString, getDBInfo, dumpToDB, toTimeStamp, updateDBInfo, addRowsToDB
import datetime as dt
import json

exchange = ccxt.binance({
    'enableRateLimit': True
})

msec = 1000
minute = 60 * msec
hour = 60 * minute

now = exchange.milliseconds()

def getMarketData(symbol, from_timestamp, limit, timeframe):
    if isinstance(from_timestamp, str):
        from_timestamp = stringToTimeStamp(from_timestamp)
    try:
        market_data = exchange.fetch_ohlcv( 
            symbol=symbol, 
            timeframe=timeframe,
            limit=limit, 
            since=from_timestamp
        )

        for data in market_data:
            data[0] = timestampToString(data[0])
        headers = ['time', 'open', 'high', 'low', 'close', 'volume', 'symbol']
        
        out_data = []
        for data_point in market_data:
            data_point.append(symbol)
            out_data.append({k:v for k,v in zip(headers, data_point)})
        
        return out_data

    except Exception as e:
        print(e)

def updateMarketDB(assets=[]):
    end_date = toTimeStamp(dt.datetime.utcnow())
    db_name = db = MARKET_DATA_PATH / 'hourly_market_data'
    db_info = getDBInfo(db_name)
    initial_date = db_info['initial_date']
    headers = db_info['headers']

    limit = 1000
    timeframe = '1h'
    current_assets = db_info['current_assets']
    full_dump = False

    assets_to_update = current_assets if assets == [] else assets
    
    start_date = initial_date if db_info['last_update_date'] == '' else db_info['last_update_date']
    full_dump = True if db_info['last_update_date'] == '' else False
    
    starting_timestamp = stringToTimeStamp(start_date)
    market_data = []

    for symbol in assets_to_update:
        from_timestamp = starting_timestamp
        while (from_timestamp < end_date):
            data_for_symbol = getMarketData(symbol, from_timestamp, limit, timeframe)
            if data_for_symbol:
                from_timestamp = stringToTimeStamp(data_for_symbol[-1]['time']) + minute
                market_data.extend(data_for_symbol)
            else:
                from_timestamp += hour * 1000

    if full_dump:
        dumpToDB(db, market_data, headers)
    else:
        addRowsToDB(db, market_data, headers)
    
    db_info = {'initial_date': initial_date, 'last_update_date': market_data[-1]['time'], "current_assets": current_assets, 'headers': headers}
        
    updateDBInfo(db, db_info)

