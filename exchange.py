
import json
from binance.client import Client
import time
import copy

from tools import gen_date_pairs, stringToTimeStamp
from exchange_actions import *
from global_vars import EXCHANGE_CODES

class BinanceSymbol():
    def __init__(self, symbol, q_curr, b_curr):
        self.symbol = symbol
        self.q_curr = q_curr
        self.b_curr = b_curr
        pass
    
    @classmethod
    def fromSymbol(cls, symbol, base_coins):
        ob = cls.__new__(cls)
        ob.symbol = symbol

        for base in base_coins:
            symbol_split = symbol.split(base)
            if len(symbol_split) > 1 and symbol_split[0] != '':
                ob.b_curr = base
                ob.q_curr = symbol_split[0]

        return ob

class Exchange(object):

    def __init__(self, code, pkey, skey, is_default) -> None:
        self.code = code
        self.exchange_name = {v:k for k, v in EXCHANGE_CODES.items()}[code]
        
        if code == 'e0001':
            self.exchange = BinanceExchange(pkey, skey)   
            self.public_key = pkey
            self.secret_key = skey         
            self.is_default = is_default
        else:
            raise Exception('Support for {} Exchange is comming soon!'.format(self.exchange_name))

    @classmethod
    def from_dict(cls, exchange_setup):
        return cls(exchange_setup['code'], exchange_setup['public_key'], exchange_setup['secret_key'], exchange_setup['is_default'])
    
    def toDict(self):
        outdict = copy.deepcopy(self.__dict__)
        outdict.pop('exchange')
        return outdict

    def getCurrentHoldings(self):
        return self.exchange.getCurrentHoldings()
    
    def getCurrentPriceForAssets(self, assets, base_currency):
        assets = [asset if asset != 'BUSD' else 'USDT' for asset in assets ]
        symbols = [asset + base_currency for asset in assets if asset != base_currency]
        prices = self.exchange.getCurrentPrice(symbols)
        out_dict = {k.split(base_currency)[0]: v for k,v in prices.items()}
        out_dict.update({base_currency: 1.0})
        return out_dict

    def getTrades(self, start_date, end_date, assets):
        exchange_trades = self.exchange.getTrades(assets, start_date, end_date)
        trades = [TradeAction(trade).toDict() for trade in exchange_trades]
        return trades
    
    def getDeposits(self, start_date, end_date):
        exchange_deposits = self.exchange.getDeposits(start_date, end_date)
        deposits = [DepositAction(deposit).toDict() for deposit in exchange_deposits]
        return deposits

    def getWithdrawals(self, start_date, end_date):
        exchange_withdrawals = self.exchange.getWithdrawals(start_date, end_date)
        withdrawals = [WithdrawlAction(withdrawal).toDict() for withdrawal in exchange_withdrawals]
        return withdrawals

    def getFiatTransactions(self, start_date, end_date):
        exchange_fiat_transactions = self.exchange.getFiatTransactions(start_date, end_date)
        fiat_transactions = [FiatDepositAction(fiat).toDict() for fiat in exchange_fiat_transactions]
        return fiat_transactions

    def getAccountDust(self, start_date, end_date):
        exchange_dust = self.exchange.getAccountDust(start_date, end_date)
        dust = [DustSweepAction(dust).toDict() for dust in exchange_dust]
        return dust

    def getAccountDividends(self, start_date, end_date):
        exchange_dividends = self.exchange.getAccountDividends(start_date, end_date)
        dividends = [DividendAction(dividend).toDict() for dividend in exchange_dividends]
        return dividends
    
    def getAccountConversions(self, start_date, end_date):
        exchange_conversions = self.exchange.getAccountConversions(start_date, end_date)
        conversions = [ConversionAction(conversion).toDict() for conversion in exchange_conversions]
        return conversions

class BinanceExchange(object):

    def __init__(self, public_key, secret_key) -> None:
        self.client = Client(public_key, secret_key)
        self.valid_symbols = [ticker['symbol'] for ticker in self.client.get_all_tickers()]
        self.base_symbols = [
            'AUD', 'BIDR', 'BRL', 'EUR', 'GBP', 'RUB', 'TRY', 'TUSD', 'USDC',
            'DAI', 'IDRT', 'UAH', 'NGN', 'VAI', 'USDP', 'BUSD', 'BNB',
            'BTC', 'USDT', 'BNB', 'ETH', 'TRX', 'XRP', 'DOGE', 'BVND']

    def getTradingSymbols(self, quote_coins):
        # TODO(ion): Speed this up by removing the reverse symbols careful with the priority of bases
        symbols = []
        
        if quote_coins:
            for q_coin in quote_coins:
                for b_coin in self.base_symbols:
                    if q_coin == b_coin:
                        continue
                    else:
                        symbol = BinanceSymbol(q_coin + b_coin, q_coin, b_coin)
                        symbols.append(symbol)
            return [symbol for symbol in symbols if symbol.symbol in self.valid_symbols]
        
        else:
            return [BinanceSymbol.fromSymbol(symbol, self.base_symbols) for symbol in self.valid_symbols]
        
    
    def getCurrentHoldings(self):
        balances = self.client.get_account()['balances']
        holdings = {}
        for asset in balances:
            asset_amount = float(asset['free']) + float(asset['locked'])
            if asset_amount > 0:
                holdings[asset['asset']] = asset_amount

        return holdings
    
    def getCurrentPrice(self, symbols) -> dict:
        prices = {}
        for symbol in symbols:
            if symbol in self.valid_symbols:
                prices[symbol] = float(self.client.get_avg_price(symbol=symbol)['price'])
            else:
                print('Warning symbol {} not valid - skipping!')
                continue

        return prices
    
    def getTrades(self, assets, start_date, end_date):
        
        # my_symbols = self.getTradingSymbols(assets)
        # start_date = stringToTimeStamp(start_date)
        # end_date = stringToTimeStamp(end_date)
        # out_trades = []
        
        # for symbol in my_symbols:
        #     trades = self.client.get_my_trades(symbol=symbol.symbol)
        #     time.sleep(0.5)
        #     if trades:
        #         for trade in trades:
        #             trade.update({'coin': symbol.q_curr})
        #             if trade['time'] > start_date and trade['time'] <= end_date:
        #                 out_trades.append(trade)
        
        with open('file.json') as infile:
            out_trades = json.load(infile)

        return out_trades
    
    def getDeposits(self, start_date, end_date):
        
        request_dates = gen_date_pairs(start_date, end_date, out_type='timestamp')
        
        deposits = []
        for i_date, j_date in request_dates:
            deposit = self.client.get_deposit_history(startTime=i_date, endTime=j_date)
            if deposit:
                deposits.extend(deposit)

        return deposits
    
    def getWithdrawals(self, start_date, end_date):

        date_pairs = gen_date_pairs(start_date, end_date, out_type='timestamp')

        withdrawals = []

        for i_date, j_date in date_pairs:
            order = {'startTime':i_date, 'endTime': j_date}
            withdrawl = self.client.get_withdraw_history(**order)
            
            if withdrawl:
                withdrawals.extend(withdrawl)
        
        return withdrawals
    
    def getFiatTransactions(self, start_date, end_date):
        
        date_pairs = gen_date_pairs(start_date, end_date, out_type='timestamp')

        fiat_transactions = []

        for i_date, j_date in date_pairs:
            fiat_transaction = self.client.get_fiat_deposit_withdraw_history(transactionType=0, beginTime=i_date, endTime=j_date)
            if fiat_transaction['total'] > 0:
                for data in fiat_transaction['data']:
                    if data['status'] == 'Successful':
                        fiat_transactions.append(data)
            
            fiat_transaction = self.client.get_fiat_deposit_withdraw_history(transactionType=1, beginTime=i_date, endTime=j_date)
            if fiat_transaction['total'] > 0:
                for data in fiat_transaction['data']:
                    if data['status'] == 'Successful':
                        fiat_transactions.append(data)
        
        return fiat_transactions
    
    def getAccountDust(self, start_date, end_date):

        order = {'startTime': stringToTimeStamp(start_date), 'endTime': stringToTimeStamp(end_date)}

        dust_activities = []
        dust_activity = self.client.get_dust_log(**order)
        if dust_activity.get('total', 0.0) > 0.0:
            for action in dust_activity['userAssetDribblets']:
                dust_activities.extend(action['userAssetDribbletDetails'])
        return dust_activities
    
    def getAccountDividends(self, start_date, end_date):

        order = {'startTime': stringToTimeStamp(start_date), 'endTime': stringToTimeStamp(end_date)}

        dividends = self.client.get_asset_dividend_history(**order)
        
        if dividends.get('total', 0.0) > 0.0:
            return dividends['rows']
        else:
            return []

    def getAccountConversions(self, start_date, end_date):
        
        date_pairs = gen_date_pairs(start_date, end_date, freq=30, out_type='timestamp')

        conversion_activities = []
        for i_date, j_date in date_pairs:
            conversions = self.client.get_conversion_history(startTime=i_date, endTime=j_date)

            if conversions['list']:
                conversion_activities.extend(conversions['list'])
        
        return conversion_activities