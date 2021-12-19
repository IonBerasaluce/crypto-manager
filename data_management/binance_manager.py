from binance.client import Client
import datetime as dt

from config import config
from utils import gen_90d_dates, toTimeStamp

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
        self.base_coins = ['BTC', 'USDT', 'BUSD', 'GBP', 'ETH']
        self.valid_symbols = [ticker['symbol'] for ticker in self.client.get_all_tickers()]

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
        order = {}

        for symbol in trading_symbols:
            order.update({'symbol':symbol.symbol})
            trades = self.client.get_my_trades(**order)
                
            if trades:
                for trade in trades:
                    trade.update({'coin': symbol.q_curr})
                    if s_date != None and e_date != None:
                        if trade['time'] > s_date and trade['time'] <= e_date:
                            latest_trades.append(trade)
                    else:
                        latest_trades.append(trade)
        
        return latest_trades

    def getDeposits(self, s_date=None, e_date=None):
        
        if s_date == None and e_date == None:
            date = toTimeStamp(dt.datetime.strptime(config.DEFAULT_START_DATE, '%Y-%m-%d'))
            request_dates = gen_90d_dates(date)
        else:
            request_dates = gen_90d_dates(s_date, e_date)
        
        deposits = []
        for i_date, j_date in request_dates:
            deposit = self.client.get_deposit_history(startTime=i_date, endTime=j_date)
            if deposit:
                deposits.extend(deposit)

        return deposits

    def getFiatDepositsWithdrawals(self, s_date=None, e_date=None):

        if s_date == None and e_date == None:
            date = toTimeStamp(dt.datetime.strptime(config.DEFAULT_START_DATE, '%Y-%m-%d'))
            date_pairs = gen_90d_dates(date)
        else:
            date_pairs = gen_90d_dates(s_date, e_date)

        deposits = []
        for i_date, j_date in date_pairs:
            deposit = self.client.get_fiat_deposit_withdraw_history(transactionType=0, beginTime=i_date, endTime=j_date)
            if deposit['total'] > 0:
                for data in deposit['data']:
                    if data['status'] == 'Successful':
                        deposits.append(data)
        
        return deposits

    def getWithdrawals(self, s_date=None, e_date=None):

        if s_date == None:
            date = toTimeStamp(dt.datetime.strptime(config.DEFAULT_START_DATE, '%Y-%m-%d'))
            date_pairs = gen_90d_dates(date) 
        else:
            date_pairs = gen_90d_dates(s_date, e_date)

        withdrawals = []

        for i_date, j_date in date_pairs:
            order = {'startTime':i_date, 'endTime': j_date}
            withdrawl = self.client.get_withdraw_history(**order)
            
            if withdrawl:
                withdrawals.extend(withdrawl)
        
        return withdrawals

    def getAccountDust(self, s_date=None, e_date=None):

        if s_date != None and e_date != None:
            order = {'startTime': s_date, 'endTime': e_date}
        else:
            order = {}

        dust_activities = []
        dust_activity = self.client.get_dust_log(**order)
        if dust_activity.get('total', 0.0) > 0.0:
            for action in dust_activity['userAssetDribblets']:
                dust_activities.extend(action['userAssetDribbletDetails'])
        return dust_activities

    def getAccountDividends(self, s_date=None, e_date=None):

        if s_date != None and e_date != None:
            order = {'startTime': s_date, 'endTime': e_date}
        else:
            order = {}

        dividends = self.client.get_asset_dividend_history(**order)
        
        if dividends.get('total', 0.0) > 0.0:
            return dividends['rows']
        else:
            return []


