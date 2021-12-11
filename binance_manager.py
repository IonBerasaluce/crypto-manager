from binance.client import Client
import datetime as dt

import config
from utils import gen_90d_dates, toTimeStamp

#TODO(Ion): Add the start date, end date functionality to these functions

class BinanceSymbol():
    def __init__(self, symbol, q_curr, b_curr):
        self.symbol = symbol
        self.q_curr = q_curr
        self.b_curr = b_curr
        pass

class BinanceAccountManager():

    def __init__(self, coin_list):
        self._key = config.API_KEY
        self._secretKey = config.API_SECRET
        self.client = Client(self._key, self._secretKey)
        self.quote_coins = coin_list
        self.base_coins = ['BTC', 'USDT', 'GBP', 'ETH']
        self.valid_symbols = [item['symbol'] for item in self.client.get_exchange_info()['symbols']]

        pass

    def getTradingSymbols(self):
        # TODO(ion): Speed this up by removing the reverse symbols careful with the priority of bases
        symbols = []
        for q_coin in self.quote_coins:
            for b_coin in self.base_coins:
                if q_coin == b_coin:
                    continue
                else:
                    symbol = BinanceSymbol(q_coin + b_coin, q_coin, b_coin)
                    symbols.append(symbol)
        
        return [symbol for symbol in symbols if symbol.symbol in self.valid_symbols]

    def getTrades(self, s_date=None, e_date=None):
        
        latest_trades = []
        trading_symbols = self.getTradingSymbols()

        for symbol in trading_symbols:
            trades = self.client.get_my_trades(symbol=symbol.symbol)
                
            if trades:
                for trade in trades:
                    trade.update({'coin': symbol.q_curr})
                
                latest_trades.extend(trades)
        
        return latest_trades

    def getDeposits(self, s_date=None):
        
        if s_date == None:
            date = dt.datetime.strptime(config.DEFAULT_START_DATE, '%Y-%m-%d')
        else:
            date = s_date

        request_dates = gen_90d_dates(date)
        deposits = []
        for i_date, j_date in request_dates:
            deposit = self.client.get_deposit_history(startTime=toTimeStamp(i_date), endTime=toTimeStamp(j_date))

            if deposit:
                deposits.extend(deposit)

        return deposits

    def getFiatDepositsWithdrawals(self, s_date=None):
        date_pairs = gen_90d_dates(dt.datetime.strptime(config.DEFAULT_START_DATE, '%Y-%m-%d'))
        deposits = []
        
        for begin, end in date_pairs:
            deposit = self.client.get_fiat_deposit_withdraw_history(transactionType=0, beginTime=toTimeStamp(begin), endTime=toTimeStamp(end))
            
            if deposit['total'] > 0:
                for data in deposit['data']:
                    if data['status'] == 'Successful':
                        deposits.append(data)
        
        return deposits

    def getWithdrawals(self, s_date=None):

        if s_date == None:
            date = dt.datetime.strptime(config.DEFAULT_START_DATE, '%Y-%m-%d')
        else:
            date = s_date

        request_dates = gen_90d_dates(date)
        withdrawals = []

        for i_date, j_date in request_dates:
            withdrawl = self.client.get_withdraw_history(startTime=toTimeStamp(i_date), endTime=toTimeStamp(j_date))
            
            if withdrawl:
                withdrawals.extend(withdrawl)
        
        return withdrawals

    def getAccountDust(self, s_date=None):
        dust_activity = self.client.get_dust_log()
        dust_activities = []
        if dust_activity['total'] > 0:
            for action in dust_activity['userAssetDribblets']:
                dust_activities.extend(action['userAssetDribbletDetails'])
        return dust_activities

    def getAccountDividends(self, s_date):
        dividends = self.client.get_asset_dividend_history()
        if dividends['total'] > 0:
            return dividends['rows']

