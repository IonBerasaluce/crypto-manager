from utils import toTimeStamp
import datetime as dt

BINANCE_KEY_MAP = {
    'coin':                   'asset',
    'amount':                 'amount',
    'insertTime':             'time',
    'network':                'network',
    'address':                'address',
    'status':                 'status',
    'txId':                   'id',
    'qty':                    'amount',
    'time':                   'time',
    'isBuyer':                'isBuyer',
    'symbol':                 'symbol',
    'price':                  'price',
    'commission':             'fee',
    'commissionAsset':        'feeAsset',
    'id':                     'id',
    'applyTime':              'time',
    'transactionFee':         'fee',
    'fromAsset':              'asset',
    'operateTime':            'time',
    'serviceChargeAmount':    'fee',
    'transferedAmount':       'transferedAmount',
    'transId':                'id',
    'fiatCurrency':           'asset',
    'createTime':             'time',
    'totalFee':               'fee',
    'orderNo':                'id',
    'divTime':                'time',
    'asset':                  'asset'
    }
def re_key_input(dictionary):
    out_dict = {BINANCE_KEY_MAP[exchange_key]: dictionary[exchange_key] for exchange_key in BINANCE_KEY_MAP.keys() if dictionary.get(exchange_key, None) != None}
    return out_dict

class ExchangeAccountAction():

    def __init__(self, asset, amount, time, description):

        self.asset = asset
        self.amount = float(amount)
        self.time = time
        self.description = description
        
        pass

    def toBaseDict(self):
        return {'asset': self.asset, 'amount': self.amount, 'time': self.time, 'description': self.description}

    def toDict(self):
        return self.__dict__
    
class TradeAction(ExchangeAccountAction):

    def __init__(self, trade_details, re_key=True):
        
        if re_key:
            trade_details = re_key_input(trade_details)

        asset = trade_details['asset']
        amount = float(trade_details['amount'])
        time = trade_details['time']
        description = 'trading activity'

        if trade_details.get('isBuyer', None) != None:
            if not trade_details['isBuyer']:
                amount = amount * -1       
        
        self.symbol = trade_details['symbol']
        self.price = float(trade_details['price'])
        self.base = self.symbol.split(asset)[1]
        self.fee = float(trade_details['fee'])
        self.feeAsset = trade_details['feeAsset']
        self.id = trade_details['id']
        
        super().__init__(asset, amount, time, description)

    def getOppositeLegAction(self):
        return ExchangeAccountAction(self.base, -1 * (self.price * self.amount), self.time, 'trading activity')

class DepositAction(ExchangeAccountAction):

    def __init__(self, deposit_details, re_key=True):

        if re_key:
            deposit_details = re_key_input(deposit_details)
        
        asset = deposit_details['asset']
        amount = float(deposit_details['amount'])
        time = deposit_details['time']
        description = 'deposit activity'

        self.network = deposit_details['network']
        self.address = deposit_details['address']
        self.status = deposit_details['status']
        self.id = deposit_details['id'] 

        super().__init__(asset, amount, time, description)
        
class WithdrawlAction(ExchangeAccountAction):

    def __init__(self, withdraw_details, re_key=True):

        if re_key:
            withdraw_details = re_key_input(withdraw_details)            

        asset = withdraw_details['asset']
        amount = float(withdraw_details['amount']) * -1 if float(withdraw_details['amount']) > 0 else float(withdraw_details['amount'])
        time = withdraw_details['time'] if isinstance(withdraw_details['time'], int) else toTimeStamp(dt.datetime.strptime(withdraw_details['time'], '%Y-%m-%d %H:%M:%S'))
        description = 'withdrawl activity'

        self.network = withdraw_details['network']
        self.address = withdraw_details['address']
        self.status = withdraw_details['status']
        self.fee = float(withdraw_details['fee']) if isinstance(withdraw_details['fee'], str) else withdraw_details['fee']
        self.feeAsset = asset
        self.id = withdraw_details['id'] 

        super().__init__(asset, amount, time, description)

class DustSweepAction(ExchangeAccountAction):

    def __init__(self, dust_details, re_key=True):

        if re_key:
            dust_details = re_key_input(dust_details)
        
        asset = dust_details['asset']
        amount = float(dust_details['amount']) * -1 if float(dust_details['amount']) > 0 else float(dust_details['amount'])
        time = dust_details['time']
        description = 'dust sweep activity'

        self.fee = float(dust_details['fee'])
        self.feeAsset = 'BNB'
        self.transferedAmount = float(dust_details['transferedAmount'])
        self.transferedAsset = 'BNB'
        self.id = dust_details['id'] 

        super().__init__(asset, amount, time, description)

    def getTransferAction(self):
        return ExchangeAccountAction(self.transferedAsset, self.transferedAmount, self.time, 'dust exchange reward')

class FiatDepositAction(ExchangeAccountAction):

    def __init__(self, fdeposit_details, re_key=True):

        if re_key:
            fdeposit_details = re_key_input(fdeposit_details)

        asset = fdeposit_details['asset']
        amount = float(fdeposit_details['amount'])
        time = fdeposit_details['time']
        description = 'fiat deposit activity'

        self.fee = float(fdeposit_details['fee'])
        self.feeAsset = asset 
        self.id = fdeposit_details['id']
                
        super().__init__(asset, amount, time, description)

class DividendAction(ExchangeAccountAction):

    def __init__(self, dividend_details, re_key=True):

        if re_key:
            dividend_details = re_key_input(dividend_details)

        asset = dividend_details['asset']
        amount = float(dividend_details['amount'])
        time = dividend_details['time']
        description = 'dividend payment'

        super().__init__(asset, amount, time, description)

class ConversionAction(ExchangeAccountAction):
    
    def __init__(self, conversion_details, re_key=True):

        if re_key:
            conversion_details = re_key_input(conversion_details)

        asset = conversion_details['asset']
        amount = float(conversion_details['amount'])
        time = conversion_details['time']
        description = 'conversion action'

        self.symbol = conversion_details['symbol']
        self.base = self.symbol.split(asset)[0]
        self.price = float(conversion_details['price'])

        super().__init__(asset, amount, time, description)

    def getOppositeLegAction(self):
        return ExchangeAccountAction(self.base, -1 * (self.price * self.amount), self.time, 'conversion activity')

class FeeAction(ExchangeAccountAction):

    def __init__(self, asset, amount, time, description):

        amount = amount * -1
        time = time
        
        super().__init__(asset, amount, time, description)