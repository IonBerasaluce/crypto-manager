
from binance_manager import BinanceAccountManager
from utils import toDate
import datetime as dt

class ExchangeAccountAction():

    def __init__(self, asset, amount, time, identifier):

        self.asset = asset
        self.amount = float(amount)
        self.time = time
        self.identifier = identifier
        
        pass

    def toBaseDict(self):
        return {'asset': self.asset, 'amount': self.amount, 'time': self.time, 'identifier': self.identifier}

    def toDict(self):
        return self.__dict__

class TradeAction(ExchangeAccountAction):

    def __init__(self, trade_details):

        asset = trade_details['coin']
        amount = float(trade_details['qty'])
        time = toDate(trade_details['time'])
        identifier = trade_details['id'] 
        
        self.symbol = trade_details['symbol']
        self.price = float(trade_details['price'])
        self.base = self.symbol.split(asset)[1]
        self.fee = float(trade_details['commission'])
        self.feeAsset = trade_details['commissionAsset']       
        
        super().__init__(asset, amount, time, identifier)

class DepositAction(ExchangeAccountAction):

    def __init__(self, deposit_details):
        
        asset = deposit_details['coin']
        amount = float(deposit_details['amount'])
        time = toDate(deposit_details['insertTime'])
        identifier = deposit_details['txId'] 
        
        self.network = deposit_details['network']
        self.address = deposit_details['address']
        self.status = deposit_details['status']

        super().__init__(asset, amount, time, identifier)
        
class WithdrawlAction(ExchangeAccountAction):

    def __init__(self, withdraw_details):

        asset = withdraw_details['coin']
        amount = float(withdraw_details['amount'])
        time = dt.datetime.strptime(withdraw_details['applyTime'], '%Y-%m-%d %H:%M:%S').date()
        identifier = withdraw_details['id'] 
        
        self.network = withdraw_details['network']
        self.address = withdraw_details['address']
        self.status = withdraw_details['status']
        self.fee = float(withdraw_details['transactionFee'])

        super().__init__(asset, amount, time, identifier)

class DustSweepAction(ExchangeAccountAction):

    def __init__(self, dust_details):

        asset = dust_details['fromAsset']
        amount = float(dust_details['amount']) * -1
        time = toDate(dust_details['operateTime'])
        identifier = dust_details['transId'] 
        
        self.fee = float(dust_details['serviceChargeAmount'])
        self.feeAsset = 'BNB'
        self.transferedAmount = float(dust_details['transferedAmount'])
        self.transferedAsset = 'BNB'

        super().__init__(asset, amount, time, identifier)

class FiatDepositAction(ExchangeAccountAction):

    def __init__(self, fdeposit_details):
        asset = fdeposit_details['fiatCurrency']
        amount = float(fdeposit_details['amount']) * -1
        time = toDate(fdeposit_details['createTime'])
        identifier = fdeposit_details['orderNo']

        self.fee = fdeposit_details['totalFee'] 
                
        super().__init__(asset, amount, time, identifier)