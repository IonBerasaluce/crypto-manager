
import json
import datetime as dt
import pandas as pd
import os

from config.config import SETUP_PATH
from utils import readCSV, createProject, toTimeStamp
from requests_db import *
from exchange_manager import *

BASE_PATH = os.getcwd()

def updateExchangeDBs(coin_list, last_update_date):
    directory = BASE_PATH + '\\data\\exchange_data\\'
    exchange_manager = ExchangeManager(coin_list, directory)
    
    start_date = toTimeStamp(dt.datetime.strptime(last_update_date, '%Y-%m-%d %H:%M:%S'))
    end_date = toTimeStamp(dt.datetime.now())

    for db in os.listdir(directory):
        if readCSV(directory + db).empty:
            exchange_manager.updateDBs(db, None, None)
        else:
            if start_date == end_date:
                print('Database {} is up to date!'.format(db))
            else:     
                exchange_manager.updateDBs(db, start_date, end_date)
    
    return end_date

def updateDerivedDBs(my_coins, last_update_date):

    db_name = 'account_movements.csv'
    directory =  BASE_PATH + '\\data\\derived_data\\'
    db = readCSV(directory + db_name, index=None, as_type=str)

    if not db.empty:
        db = db.set_index('time')
    
    start_date = toTimeStamp(dt.datetime.strptime(last_update_date, '%Y-%m-%d %H:%M:%S')) if not db.empty else None
    end_date = toTimeStamp(dt.datetime.now()) if not db.empty else None
    
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
        
        trades = exchange_manager.getAdditionalInfo(trades)
        fiat = exchange_manager.getAdditionalInfo(fiat)
        withdrawals = exchange_manager.getAdditionalInfo(withdrawals)
        dust = exchange_manager.getAdditionalInfo(dust)
        conversions = exchange_manager.getAdditionalInfo(conversions)

        trades = genEntries(trades)
        deposits = genEntries(deposits)
        fiat = genEntries(fiat)
        withdrawals = genEntries(withdrawals)
        dust = genEntries(dust)
        dividends = genEntries(dividends)
        conversions = genEntries(conversions)
        
        db = pd.concat([db, trades, deposits, fiat, withdrawals, dust, dividends, conversions], axis=0, sort=False).sort_index()
        db.to_csv(directory + db_name)

        print('Generated all moves for user!')
    
    else:
        print('Database {} is up to date!'.format(db))


with open(SETUP_PATH) as infile:
    setup = json.load(infile)

createProject(BASE_PATH)

updateExchangeDBs(setup['my_coins'], setup['last_update_date'])
updateDerivedDBs(setup['my_coins'], setup['last_update_date'])


    