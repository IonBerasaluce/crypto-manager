from utils import toDate
import datetime as dt

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

    def __init__(self, trade_details):

        asset = trade_details['coin']
        amount = float(trade_details['qty'])
        time = toDate(trade_details['time'])
        description = 'trading activity'
        
        if not trade_details['isBuyer']:
            amount = amount * -1       
        
        self.symbol = trade_details['symbol']
        self.price = float(trade_details['price'])
        self.base = self.symbol.split(asset)[1]
        self.fee = float(trade_details['commission'])
        self.feeAsset = trade_details['commissionAsset']
        self.identifier = trade_details['id'] 
        
        super().__init__(asset, amount, time, description)

    def getOppositeLegAction(self):
        return ExchangeAccountAction(self.base, -1 * (self.price * self.amount), self.time, 'trading activity')

class DepositAction(ExchangeAccountAction):

    def __init__(self, deposit_details):
        
        asset = deposit_details['coin']
        amount = float(deposit_details['amount'])
        time = toDate(deposit_details['insertTime'])
        description = 'deposit activity'

        self.network = deposit_details['network']
        self.address = deposit_details['address']
        self.status = deposit_details['status']
        self.identifier = deposit_details['txId'] 

        super().__init__(asset, amount, time, description)
        
class WithdrawlAction(ExchangeAccountAction):

    def __init__(self, withdraw_details):

        asset = withdraw_details['coin']
        amount = float(withdraw_details['amount']) * -1
        time = dt.datetime.strptime(withdraw_details['applyTime'], '%Y-%m-%d %H:%M:%S').date()
        description = 'withdrawl activity'

        self.network = withdraw_details['network']
        self.address = withdraw_details['address']
        self.status = withdraw_details['status']
        self.fee = float(withdraw_details['transactionFee'])
        self.feeAsset = asset
        self.identifier = withdraw_details['id'] 

        super().__init__(asset, amount, time, description)

class DustSweepAction(ExchangeAccountAction):

    def __init__(self, dust_details):

        asset = dust_details['fromAsset']
        amount = float(dust_details['amount']) * -1
        time = toDate(dust_details['operateTime'])
        description = 'dust sweep activity'

        self.fee = float(dust_details['serviceChargeAmount'])
        self.feeAsset = 'BNB'
        self.transferedAmount = float(dust_details['transferedAmount'])
        self.transferedAsset = 'BNB'
        self.identifier = dust_details['transId'] 

        super().__init__(asset, amount, time, description)

    def getTransferAction(self):
        return ExchangeAccountAction(self.transferedAsset, self.transferedAmount, self.time, 'dust exchange reward')

class FiatDepositAction(ExchangeAccountAction):

    def __init__(self, fdeposit_details):
        asset = fdeposit_details['fiatCurrency']
        amount = float(fdeposit_details['amount'])
        time = toDate(fdeposit_details['createTime'])
        description = 'fiat deposit activity'

        self.fee = float(fdeposit_details['totalFee'])
        self.feeAsset = asset 
        self.identifier = fdeposit_details['orderNo']
                
        super().__init__(asset, amount, time, description)

class DividendAction(ExchangeAccountAction):

    def __init__(self, dividend_details):

        asset = dividend_details['asset']
        amount = float(dividend_details['amount'])
        time = toDate(dividend_details['divTime'])
        description = 'dividend payment'

        super().__init__(asset, amount, time, description)

class FeeAction(ExchangeAccountAction):

    def __init__(self, asset, amount, time, description):

        amount = amount * -1
        time = time
        
        super().__init__(asset, amount, time, description)