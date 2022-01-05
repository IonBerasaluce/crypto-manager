'''
layout of this service
2 files for each "database"
    1. Json file - this will contain the following:
        1. Last update date
        2. Data fields - mainly this is for handling market data and the historical holdings to quickly know if we have started a new position

Must be able to run hourly (hourly market data), run upon new user creation, run overnight
'''

import json
import datetime as dt

from exchange import Exchange
from exchange_actions import *
from global_vars import *
from tools import getDBInfo, dumpToDB, updateDBInfo, addRowsToDB, dbRead, constructHistoricalHoldingsFromActions

with open(SETUP_DIR) as infile:
    SETUP = json.load(infile)


def loadAllDataFromExchange(user, exchange):
    end_date = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    
    # Update trades
    db = USER_DATA_PATH / (user + '/historical_data/historical_trades') 

    db_info = getDBInfo(db)
    start_date = db_info[exchange.code]['last_update_date']
    headers = db_info['headers']
    assets_traded = db_info[exchange.code]['assets_traded']
    full_download = False

    # TODO(): User may have created an account but not traded which will force an update here and will take very long! must fix!
    if start_date == "":
        start_date = "2017-01-01 00:00:00"
        full_download = True
    else:    
        current_assets = exchange.getCurrentHoldings().keys()    
        new_assets = [asset for asset in current_assets if asset not in assets_traded]
        assets_traded.extend(new_assets)
    
    new_trades = exchange.getTrades(start_date, end_date, assets_traded)

    if not assets_traded:
        assets_traded = list(set([trade['asset'] for trade in new_trades]))

    if full_download:
        dumpToDB(db, new_trades, headers)
    else:
        addRowsToDB(db, new_trades, headers)
    
    out_dict = {exchange.code: {'last_update_date': end_date, 'assets_traded': assets_traded}, 'headers': headers}

    updateDBInfo(db, out_dict)
    
    # Update deposits
    db = USER_DATA_PATH / (user + '/historical_data/historical_deposits')
    db_info = getDBInfo(db)
    start_date = db_info[exchange.code]['last_update_date']
    headers = db_info['headers']
    full_download = False

    if start_date == "":
        start_date = "2017-01-01 00:00:00"
        full_download = True
    
    new_deposits = exchange.getDeposits(start_date, end_date)

    if full_download:
        dumpToDB(db, new_deposits, headers)
    else:
        addRowsToDB(db, new_deposits, headers)

    out_dict = {exchange.code: {'last_update_date': end_date}, 'headers': headers}

    updateDBInfo(db, out_dict)

    # Update withdrawals
    db = USER_DATA_PATH / (user + '/historical_data/historical_withdrawals')
    db_info = getDBInfo(db)
    start_date = db_info[exchange.code]['last_update_date']
    headers = db_info['headers']
    full_download = False

    if start_date == "":
        start_date = "2017-01-01 00:00:00"
        full_download = True

    new_withdrawals = exchange.getWithdrawals(start_date, end_date)

    if full_download:
        dumpToDB(db, new_withdrawals, headers)
    else:
        addRowsToDB(db, new_withdrawals, headers)

    out_dict = {exchange.code: {'last_update_date': end_date}, 'headers': headers}

    updateDBInfo(db, out_dict)

    # Update fiat
    db = USER_DATA_PATH / (user + '/historical_data/historical_fiat_movements')
    db_info = getDBInfo(db)
    start_date = db_info[exchange.code]['last_update_date']
    headers = db_info['headers']
    full_download = False

    if start_date == "":
        start_date = "2017-01-01 00:00:00"
        full_download = True
    
    new_fiat = exchange.getFiatTransactions(start_date, end_date)

    if full_download:
        dumpToDB(db, new_fiat, headers)
    else:
        addRowsToDB(db, new_fiat, headers)
    
    out_dict = {exchange.code: {'last_update_date': end_date}, 'headers': headers}

    updateDBInfo(db, out_dict)

    # Update dust
    db = USER_DATA_PATH / (user + '/historical_data/historical_dust_activities')
    db_info = getDBInfo(db)
    start_date = db_info[exchange.code]['last_update_date']
    headers = db_info['headers']
    full_download = False

    if start_date == "":
        start_date = "2017-01-01 00:00:00"
        full_download = True

    new_dust = exchange.getAccountDust(start_date, end_date)

    if full_download:
        dumpToDB(db, new_dust, headers)
    else:
        addRowsToDB(db, new_dust, headers)

    out_dict = {exchange.code: {'last_update_date': end_date}, 'headers': headers}

    updateDBInfo(db, out_dict)

    # Update dividends
    db = USER_DATA_PATH / (user + '/historical_data/historical_dividends')
    db_info = getDBInfo(db)
    start_date = db_info[exchange.code]['last_update_date']
    headers = db_info['headers']
    full_download = False

    if start_date == "":
        start_date = "2017-01-01 00:00:00"
        full_download = True

    new_dividends = exchange.getAccountDividends(start_date, end_date)

    if full_download:
        dumpToDB(db, new_dividends, headers)
    else:
        addRowsToDB(db, new_dividends, headers)

    out_dict = {exchange.code: {'last_update_date': end_date}, 'headers': headers}

    updateDBInfo(db, out_dict)

    # Update conversions
    db = USER_DATA_PATH / (user + '/historical_data/historical_conversions')
    db_info = getDBInfo(db)
    start_date = db_info[exchange.code]['last_update_date']
    headers = db_info['headers']
    full_download = False

    if start_date == "":
        start_date = "2017-01-01 00:00:00"
        full_download = True
        
    new_conversions = exchange.getAccountConversions(start_date, end_date)

    if full_download:
        dumpToDB(db, new_conversions, headers)
    else:
        addRowsToDB(db, new_conversions, headers)

    out_dict = {exchange.code: {'last_update_date': end_date}, 'headers': headers}

    updateDBInfo(db, out_dict)

    return

def createHistoricalUserActions(user, all_actions=False):
    end_date = dateToString(dt.datetime.utcnow())
    db_name = USER_DATA_PATH / (user + '/historical_data/historical_movements')
    db_info = getDBInfo(db_name)
    start_date = db_info['last_update_date']

    if start_date == ''or all_actions == True:
        start_date = '2017-01-01 00:00:00'

    # Compile all the moves that the user has performed in the 
    db = USER_DATA_PATH / (user + '/historical_data/historical_trades') 
    trades = dbRead(db, start_date, end_date, combine_dates=True)
    db = USER_DATA_PATH / (user + '/historical_data/historical_deposits')
    deposits = dbRead(db, start_date, end_date, combine_dates=True)
    db = USER_DATA_PATH / (user + '/historical_data/historical_fiat_movements')
    fiat = dbRead(db, start_date, end_date, combine_dates=True)
    db = USER_DATA_PATH / (user + '/historical_data/historical_withdrawals')    
    withdrawals = dbRead(db, start_date, end_date, combine_dates=True)
    db = USER_DATA_PATH / (user + '/historical_data/historical_dust_activities')
    dust = dbRead(db, start_date, end_date, combine_dates=True)
    db = USER_DATA_PATH / (user + '/historical_data/historical_dividends')
    dividends = dbRead(db, start_date, end_date, combine_dates=True)
    db = USER_DATA_PATH / (user + '/historical_data/historical_conversions')
    conversions = dbRead(db, start_date, end_date, combine_dates=True)

    actions = []
    trades = [TradeAction(ta, re_key=False) for ta in trades.data]
    deposits = [DepositAction(da, re_key=False) for da in deposits.data]
    fiat = [FiatDepositAction(fa, re_key=False) for fa in fiat.data]
    withdrawals = [WithdrawlAction(wa, re_key=False) for wa in withdrawals.data]
    dust = [DustSweepAction(du, re_key=False) for du in dust.data]
    dividends = [DividendAction(da, re_key=False) for da in dividends.data]
    conversions = [ConversionAction(ca, re_key=False) for ca in conversions.data]

    actions.extend(trades + deposits + fiat + withdrawals + dust + dividends + conversions)
    actions = getAdditionalInformation(actions)

    return actions

def exportActions(user, all_actions, full_download):

    end_date = dateToString(dt.datetime.utcnow())
    db_name = USER_DATA_PATH / (user + '/historical_data/historical_movements')
    db_info = getDBInfo(db_name)
    headers = db_info['headers']

    out_data = [action.toBaseDict() for action in all_actions]

    if full_download:
        dumpToDB(db_name, out_data, headers)
    else:
        addRowsToDB(db_name, out_data, headers)

    out_dict = {'last_update_date': end_date, 'headers': headers}
    updateDBInfo(db_name, out_dict)

def createHistoricalUserHoldings(user, actions=[]):
    end_date = dateToString(dt.datetime.utcnow())
    if actions:
        all_actions = actions
    else:
        all_actions = createHistoricalUserActions(user, all_actions=True)
    
    historical_holdings = constructHistoricalHoldingsFromActions(all_actions)
    
    db_name = USER_DATA_PATH / (user + '/historical_data/historical_holdings')
    headers = historical_holdings.assets
    
    dumpToDB(db_name, historical_holdings.data, headers=headers)

    out_dict = {'last_update_date': end_date, 'headers': headers}
    updateDBInfo(db_name, out_dict)
    
    return

def createHistoricalUserData(user, exchange):
    loadAllDataFromExchange(user, exchange)
    user_actions = createHistoricalUserActions(user, all_actions=True)
    exportActions(user, user_actions, full_download=True)
    createHistoricalUserHoldings(user, actions=user_actions)
    return


def main():
    # TODO: Check how to avoid creating the new actions if there are no new actions in any exchange. Look at most up to date holdings compared to getCurrentHoldings()
    for user in SETUP['users']:
        # Update the data from each of the respective exchanges that
        # the user has signed up to. 
        for ex in SETUP['users'][user]['user_exchanges']:
            current_exchange = Exchange.from_dict(ex)

            loadAllDataFromExchange(user, current_exchange)

        user_actions = createHistoricalUserActions(user)
        if user_actions:
            exportActions(user, user_actions, full_download=True)
            createHistoricalUserHoldings(user, actions=user_actions)


createHistoricalUserHoldings('0001')




