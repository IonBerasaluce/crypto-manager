
import json
import datetime as dt
import pandas as pd
import os

from config.config import SETUP_PATH
from utils import toDate
from utils import readCSV, createProject, toTimeStamp
from requests_db import *
from exchange_manager import *

BASE_PATH = os.getcwd()

def updateExchangeDBs(coin_list, last_update_dates):
    directory = BASE_PATH + '\\data\\exchange_data\\'
    exchange_manager = ExchangeManager(coin_list, directory)
    
    for db in os.listdir(directory):
        start_date = last_update_dates[db]
        start_date = toTimeStamp(dt.datetime.strptime(start_date, '%Y-%m-%d'))
        end_date = toTimeStamp((dt.datetime.strptime(dt.datetime.today().date().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")))

        if readCSV(directory + db).empty:
            exchange_manager.updateDBs(db, None, None)
        else:
            if start_date == end_date:
                print('Database {} is up to date!'.format(db))
            else:     
                exchange_manager.updateDBs(db, start_date, end_date)
        
        last_update_dates[db] = toDate(end_date).strftime('%Y-%m-%d')
    
    return last_update_dates

def updateDerivedDBs(my_coins, last_update_dates):

    db_name = 'account_movements.csv'
    directory =  BASE_PATH + '\\data\\derived_data\\'
    db = readCSV(directory + db_name, index=None, as_type=str)

    if not db.empty:
        db = db.set_index('time')
    
    start_date = last_update_dates[db_name]
    start_date = toTimeStamp(dt.datetime.strptime(start_date, '%Y-%m-%d')) if not db.empty else None
    end_date = toTimeStamp((dt.datetime.strptime(dt.datetime.today().date().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"))) if not db.empty else None
    
    if start_date != end_date or start_date == None:
        exchange_manager = ExchangeManager(my_coins, directory)
        request = DBRequest(start_time=start_date, end_time=end_date)

        trades      = [TradeAction(ta,   re_key=False)     for ta in request.getTrades()]
        deposits    = [DepositAction(da, re_key=False)     for da in request.getDeposits()]
        fiat        = [FiatDepositAction(fa,re_key=False)  for fa in request.getFiatActivities()]
        withdrawals = [WithdrawlAction(wa, re_key=False)   for wa in request.getWithdrawals()]
        dust        = [DustSweepAction(da, re_key=False)   for da in request.getDustActivities()] 
        dividends   = [DividendAction(da, re_key=False)    for da in request.getDividends()]
        conversions = [ConversionAction(da, re_key=False)  for da in request.getConversionMovements()]
        
        trades = exchange_manager.getAdditionalInfo(trades) if trades else trades
        fiat = exchange_manager.getAdditionalInfo(fiat) if fiat else fiat
        withdrawals = exchange_manager.getAdditionalInfo(withdrawals) if withdrawals else withdrawals
        dust = exchange_manager.getAdditionalInfo(dust) if dust else dust
        conversions = exchange_manager.getAdditionalInfo(conversions) if conversions else conversions

        trades = genEntries(trades)
        deposits = genEntries(deposits)
        fiat = genEntries(fiat)
        withdrawals = genEntries(withdrawals)
        dust = genEntries(dust)
        dividends = genEntries(dividends)
        conversions = genEntries(conversions)
        
        db = pd.concat([db, trades, deposits, fiat, withdrawals, dust, dividends, conversions], axis=0, sort=False).sort_index()
        db.to_csv(directory + db_name)

        last_update_dates[db_name] = toDate(end_date).strftime('%Y-%m-%d')
        print('Generated all moves for user!')
    
    else:
        print('Database {} is up to date!'.format(db_name))
    
    return last_update_dates

with open(SETUP_PATH) as infile:
    setup = json.load(infile)

createProject(BASE_PATH)

setup['last_update_dates'] = updateExchangeDBs(setup['my_coins'], setup['last_update_dates'])
setup['last_update_dates'] = updateDerivedDBs(setup['my_coins'], setup['last_update_dates'])

with open(SETUP_PATH, 'w') as outfile:
    json.dump(setup, outfile, indent=4)


    