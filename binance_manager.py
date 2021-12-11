from binance.client import Client
import config
import pandas as pd
import datetime as dt
from pathlib import Path

from utils import gen_90d_dates, readCSV
'''
Problem downloading the historical trades too as we don't know the assets that are held in the account historically unless the user inputs

Problems ecountered:
    GBP, USDT weird output - this is coming from the convert history not working
    We need a workaround to download all of the trades regardless of the base currency used without looping through all coins in binance
    Handle currencies that we are sending to offline wallets - perhaps we need to whitelist certain wallet ids?? Can another user retrieve theirs??

Things to look into:
1. For the fee costs calculations we must look within the transactions (inside historical trades and factor in the comission).
2. For the FX of comission we may need to make a request to Binance API (provides data accurate to a minute)
4. Totalling fees in USDT/GBP

'''

class BinanceAccountManager():

    def __init__(self, coin_list, base_coin_list):
        self._key = config.API_KEY
        self._secretKey = config.API_SECRET
        self.client = Client(self._key, self._secretKey)
        self.my_coins = coin_list
        self.base_coins = ['BTC', 'USDT', 'GBP', 'ETH']
        self.valid_symbols = [item['symbol'] for item in self.client.get_exchange_info()['symbols']]

        return
    
    def checkSymbol(self, symbol):
        return symbol in self.valid_symbols 

    def getTrades(self, startDate=None, endDate=None):
        latest_trades = pd.DataFrame()
        descriptors = ['symbol', 'time', 'isBuyer', 'qty', 'price', 'coin']
        fee_decriptors = ['commissionAsset', 'commission', 'time']
        for asset_name in self.my_coins:
            for base_name in self.base_coins:
                symbol = asset_name+base_name
                if self.checkSymbol(symbol):
                    trades = self.client.get_my_trades(symbol=asset_name+base_name)
                else:
                    continue
                    
                if trades:
                    for trade in trades:
                        trade.update({'coin': asset_name})
                    
                    latest_trades = pd.concat([latest_trades, pd.DataFrame(trades).reindex(descriptors, axis=1)], axis=0, sort=False)
                    latest_trades = pd.concat([latest_trades, pd.DataFrame(trades).reindex(fee_decriptors, axis=1).rename({'commissionAsset': 'coin', 'commission': 'qty'}, axis=1)], axis=0, sort=False)

        latest_trades.index = [dt.datetime.fromtimestamp(date/1000) for date in latest_trades['time']]
        latest_trades = latest_trades.drop('time', axis=1)
        latest_trades.loc[:, 'qty'] = pd.to_numeric(latest_trades['qty'], errors='coerce')
        latest_trades.loc[:, 'description'] = 'trading activity'
        latest_trades.loc[:, 'isBuyer'] = latest_trades['isBuyer'].fillna(False)
        
        return latest_trades

    def getDeposits(self, s_date=None, e_date=None):
        
        descriptors = ['coin', 'amount', 'insertTime']
        if (s_date != None) and (e_date != None):
            startDate = int(dt.datetime.timestamp(s_date))*1000
            endDate = int(dt.datetime.timestamp(e_date))*1000
            deposits = self.client.get_deposit_history(startTime=startDate, endTime=endDate)
        else:
            deposits = self.client.get_deposit_history()
        
        historical_deposits = pd.DataFrame()
        
        if deposits:
            for i, deposit in enumerate(deposits):
                historical_deposits = pd.concat([historical_deposits, pd.Series(deposit, name=i)[descriptors]], axis=1, sort=False)

            historical_deposits = historical_deposits.T
            historical_deposits.index = [dt.datetime.fromtimestamp(date/1000) for date in historical_deposits['insertTime']]
            historical_deposits = historical_deposits.drop('insertTime', axis=1)

            historical_deposits.loc[:, 'amount'] = pd.to_numeric(historical_deposits['amount'], errors='coerce')
            historical_deposits.loc[:, 'description'] = 'deposit activity'

        return historical_deposits

    def getFiatDepositsWithdrawals(self):
        date_pairs = gen_90d_dates(dt.datetime.strptime(config.DEFAULT_START_DATE, '%Y-%m-%d'))
        deposits = []
        for s_date, e_date in date_pairs:
            s_date_ms = int(dt.datetime.timestamp(s_date)*1000)
            e_date_ms = int(dt.datetime.timestamp(e_date)*1000)
            deposit = self.client.get_fiat_deposit_withdraw_history(transactionType=0, beginTime=s_date_ms, endTime=e_date_ms)
            if deposit['total'] > 0:
                for data in deposit['data']:
                    if data['status'] == 'Successful':
                        deposits.append(pd.Series(data))
           
        fiat_deposits = pd.concat(deposits, axis=1, sort=False).T.set_index('createTime').reindex(['amount', 'fiatCurrency'], axis=1)
        fiat_deposits.index = pd.to_datetime(fiat_deposits.index, unit='ms').date
        fiat_deposits.amount = pd.to_numeric(fiat_deposits.amount)
        fiat_deposits.rename({'fiatCurrency': 'coin'}, axis=1, inplace=True)
        fiat_deposits.loc[:, 'description'] = 'fiat activity'

        return fiat_deposits

    def getWithdrawls(self, s_date=None, e_date=None):

        descriptors = ['coin', 'amount', 'applyTime', 'transactionFee']
        if (s_date != None) and (e_date != None):
            startDate = int(dt.datetime.timestamp(s_date))*1000
            endDate = int(dt.datetime.timestamp(e_date))*1000
            withdrawls = self.client.get_withdraw_history(startTime=startDate, endTime=endDate)
        else:
            withdrawls = self.client.get_withdraw_history()
        
        historical_withdrawls = pd.DataFrame()
        if withdrawls:
            for i, withdraw in enumerate(withdrawls):
                historical_withdrawls = pd.concat([historical_withdrawls, pd.Series(withdraw, name=i)[descriptors]], axis=1, sort=False)

            historical_withdrawls = historical_withdrawls.T.set_index('applyTime')
            historical_withdrawls.index = pd.to_datetime(historical_withdrawls.index).date
            historical_withdrawls.loc[:, 'amount'] = pd.to_numeric(historical_withdrawls['amount'], errors='coerce') + pd.to_numeric(historical_withdrawls['transactionFee'], errors='coerce')
            historical_withdrawls = historical_withdrawls.drop('transactionFee', axis=1)
            historical_withdrawls.loc[:, 'description'] = 'withdrawal activity'
        
        return historical_withdrawls

    def getAccountDust(self):
        dust_activity = acc_manager.client.get_dust_log()
        dust_transactions = pd.DataFrame()
        cols_to_keep = ['fromAsset', 'amount']
        for activity in dust_activity['userAssetDribblets']:
            
            time = dt.datetime.fromtimestamp(activity['operateTime']/1000)
            total_amount = float(activity['totalTransferedAmount']) - float(activity['totalServiceChargeAmount'])
            
            dust_transaction = pd.DataFrame(activity['userAssetDribbletDetails']).reindex(cols_to_keep, axis=1)
            dust_transaction['amount'] = pd.to_numeric(dust_transaction['amount']) * -1
            dust_transaction.index = [time] * len(dust_transaction)
            
            dust_transactions = pd.concat([dust_transactions, dust_transaction], axis=0, sort=False)
            dust_transactions = dust_transactions.append(pd.Series(data=['BNB', total_amount], index=cols_to_keep, name=time)) 

        dust_transactions.rename({'fromAsset': 'coin'}, axis=1, inplace=True)
        dust_transactions.loc[:, 'description'] = 'dust sweep activity'

        return dust_transactions

    def getAccountDividends(self):
        dividends = self.client.get_asset_dividend_history()
        dividends_df = pd.DataFrame(dividends['rows']).set_index('divTime').reindex(['asset', 'amount'], axis=1)
        dividends_df.index = pd.to_datetime(dividends_df.index, unit='ms')
        dividends_df['amount'] = pd.to_numeric(dividends_df['amount'])
        dividends_df.rename({'asset': 'coin'}, axis=1, inplace=True)

        dividends_df.loc[:, 'description'] = 'distribution activity'
        
        return dividends_df

    # This will move to the DataCleaner and we will need to create some pandas helper functions in utils class to simplify the way in which trades are saved and managed
    def getAccountMovements(self):

        default_startDate = dt.datetime.strptime(config.DEFAULT_START_DATE, '%Y-%m-%d')
        request_dates = gen_90d_dates(default_startDate)
        my_deposits = []
        my_withdrawals = []

        for startDate, endDate in request_dates:
            my_deposits.append(self.getDeposits(startDate, endDate))
            my_withdrawals.append(self.getWithdrawls(startDate, endDate))

        my_deposits = pd.concat(my_deposits, axis=0, sort=False)
        my_withdrawals = pd.concat(my_withdrawals, axis=0, sort=False)
        my_trades = self.getTrades()
        
        # Trades
        all_moves = my_trades.reindex(['coin', 'qty', 'isBuyer', 'description'], axis=1)
        mask = ~all_moves['isBuyer']
        all_moves.loc[mask, 'qty'] =  all_moves.loc[mask, 'qty'] * -1
        all_moves = all_moves.drop('isBuyer', axis=1).rename({'qty': 'amount'}, axis=1)

        # Add deposits
        all_moves = pd.concat([all_moves, my_deposits], axis=0, sort=False)

        # Remove Withdrawls
        my_withdrawals.loc[:, 'amount'] = my_withdrawals['amount'] * -1
        all_moves = pd.concat([all_moves, my_withdrawals], axis=0, sort=False)

        # Handle dust transactions
        my_dust_transactions = self.getAccountDust()
        all_moves = pd.concat([all_moves, my_dust_transactions], axis=0, sort=False)

        # Handle dividends from staking
        my_dividends = self.getAccountDividends()
        all_moves = pd.concat([all_moves, my_dividends], axis=0, sort=False).sort_index()

        # Handle fiat deposits and withdrawls
        my_fiat_deposits = self.getFiatDepositsWithdrawals()
        all_moves = pd.concat([all_moves, my_fiat_deposits], axis=0, sort=False).sort_index()

        return all_moves


# External method after data acquisition has taken place
def getMovesForCoin(coin, moves):
    coin_moves = moves.loc[moves.coin == coin, 'amount'].rename(coin)
    coin_moves.index = pd.to_datetime(coin_moves.index).date
    coin_moves = coin_moves.groupby(coin_moves.index).sum() 
    return coin_moves

my_coins = ['BTC', 'ETH', 'ADA', 'LUNA', 'AR', 'RUNE', 'DOT', 'SOL', 'OM', 'LINK', 'AVAX', 'BNB', 'MATIC', 'FTM', 'AXS', 'CAKE', 'GBP', 'CHZ']
base_list = ['BTC', 'USDT', 'GBP']

acc_manager = BinanceAccountManager(my_coins, base_list)

path = Path('account_movements.csv')

if path.is_file():
    account_movements = readCSV('account_movements.csv')
else:
    account_movements = acc_manager.getAccountMovements()
    account_movements.to_csv('account_movements.csv')


start_date = account_movements.index[0]
end_date = dt.datetime.today()

traded_coins = pd.unique(account_movements['coin'])
historical_holdings = pd.DataFrame(data=0.0, index=[start_date, end_date], columns=traded_coins).resample('D').pad().fillna(0.0)

for coin in traded_coins:
    coin_moves = acc_manager.getMovesForCoin(coin, account_movements)
    historical_holdings.loc[coin_moves.index, coin] = historical_holdings.loc[coin_moves.index, coin] + coin_moves 

historical_holdings = historical_holdings.cumsum()

# acc_manager.client.get_convert_trade_history(startTime=1627772400000, endTime=1630364400)

print('complete')


