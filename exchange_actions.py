import datetime as dt

from tools import stringToDate, dateToString, stringToTimeStamp, toTimeStamp, timestampToString, dbRead
from global_vars import *

def re_key_input(dictionary):
    out_dict = {BINANCE_KEY_MAP[exchange_key]: dictionary[exchange_key] for exchange_key in BINANCE_KEY_MAP.keys() if dictionary.get(exchange_key, None) != None}
    return out_dict

def getPriceAtTime(symbol, time):
    db_name = 'hourly_market_data.csv'
    headers = ['close', 'symbol']

    time = stringToDate(time)
    start_date = dateToString(time.replace(minute=0, second=0))
    end_date = dateToString(time.replace(minute=0, second=0) + dt.timedelta(hours=1))
    time = toTimeStamp(time)

    prices = dbRead(MARKET_DATA_PATH / db_name, start_date=start_date, end_date=end_date, headers=headers, filters=[{'symbol': symbol}])
    timestamps = [stringToTimeStamp(time) for time in prices.dates]

    estimated_price = prices.data[0]['close'] + (time - timestamps[0])*(prices.data[1]['close'] - prices.data[0]['close'])/(timestamps[1] - timestamps[0])

    return estimated_price

def getAdditionalInformation(actions):
    for action in actions:
        if type(action) == TradeAction:
            trade_fee_action = FeeAction(action.feeAsset, action.fee, action.time, 'trading fees')
            trade_base_action = action.getOppositeLegAction()
            actions.extend([trade_fee_action, trade_base_action])
        elif type(action) == FiatDepositAction:
            fiat_fee_action = FeeAction(action.feeAsset, action.fee, action.time, 'fiat deposit fee')
            actions.append(fiat_fee_action)
        elif type(action) == WithdrawlAction:
            withdrawl_fee_action = FeeAction(action.feeAsset, action.fee, action.time, 'withdrawl fees')
            actions.append(withdrawl_fee_action)
        elif type(action) == DustSweepAction:
            dust_transfer_action = action.getTransferAction()
            dust_fee_action = FeeAction(action.feeAsset, action.fee, action.time, 'dust exchange fee')
            actions.extend([dust_fee_action, dust_transfer_action])
        elif type(action) == ConversionAction:
            conversion_base_action = action.getOppositeLegAction()
            actions.append(conversion_base_action)
    return actions

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
        time = timestampToString(trade_details['time']) if isinstance(trade_details['time'], int) else trade_details['time']
        description = 'trading activity'

        if trade_details.get('isBuyer', None) != None:
            if not trade_details['isBuyer']:
                amount = amount * -1  
                self.type = 'Sell'
            else:
                self.type = 'Buy'     
        
        self.symbol = trade_details['symbol']
        self.price = float(trade_details['price'])
        base_symbol = self.symbol.split(asset)[1]
        self.base = base_symbol if base_symbol not in ['TUSD', 'USDC', 'USD', 'BUSD', 'USDP']  else 'USDT'
        
        base_price = trade_details.get('basePrice', None)
        if base_price == None:
            self.basePrice = getPriceAtTime(self.base + BASECURR, time) if self.base != BASECURR else 1.0
        else:
            self.basePrice = base_price
        
        self.fee = float(trade_details['fee'])
        self.feeAsset = trade_details['feeAsset'] if trade_details['feeAsset'] != 'BUSD' else 'USDT'
        self.id = trade_details['id']
        
        fee_asset_price = trade_details.get('feeAssetPrice', None)
        if  fee_asset_price == None:
            feeSymbol = self.feeAsset + BASECURR
            if self.feeAsset == BASECURR:
                self.feeAssetPrice = 1.0
            elif feeSymbol == self.symbol:
                self.feeAssetPrice = self.price
            else:
                self.feeAssetPrice = getPriceAtTime(feeSymbol, time)
        else:
            self.feeAssetPrice = fee_asset_price
        
        super().__init__(asset, amount, time, description)

    def getOppositeLegAction(self):
        return ExchangeAccountAction(self.base, -1 * (self.price * self.amount), self.time, 'trading activity')

class DepositAction(ExchangeAccountAction):

    def __init__(self, deposit_details, re_key=True):

        if re_key:
            deposit_details = re_key_input(deposit_details)
        
        asset = deposit_details['asset']
        amount = float(deposit_details['amount'])
        time = timestampToString(deposit_details['time']) if isinstance(deposit_details['time'], int) else deposit_details['time']
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
        time = withdraw_details['time']
        description = 'withdrawal activity'

        self.network = withdraw_details['network']
        self.address = withdraw_details['address']
        self.status = withdraw_details['status']
        self.fee = float(withdraw_details['fee']) if isinstance(withdraw_details['fee'], str) else withdraw_details['fee']
        self.feeAsset = asset

        fee_asset_price = withdraw_details.get('feeAssetPrice', None)
        if fee_asset_price == None:
            self.feeAssetPrice = getPriceAtTime(self.feeAsset + BASECURR, time) if self.feeAsset != BASECURR else self.price
        else:
            self.feeAssetPrice = fee_asset_price

        self.id = withdraw_details['id'] 

        super().__init__(asset, amount, time, description)

class DustSweepAction(ExchangeAccountAction):

    def __init__(self, dust_details, re_key=True):

        if re_key:
            dust_details = re_key_input(dust_details)
        
        asset = dust_details['asset']
        amount = float(dust_details['amount']) * -1 if float(dust_details['amount']) > 0 else float(dust_details['amount'])
        time = timestampToString(dust_details['time']) if isinstance(dust_details['time'], int) else dust_details['time']
        description = 'dust sweep activity'

        self.fee = float(dust_details['fee'])
        self.feeAsset = 'BNB'

        fee_asset_price = dust_details.get('feeAssetPrice', None)

        if fee_asset_price == None:
            self.feeAssetPrice = getPriceAtTime(self.feeAsset + BASECURR, time) if self.feeAsset != BASECURR else 1
        else:
            self.feeAssetPrice = fee_asset_price

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
        time = timestampToString(fdeposit_details['time']) if isinstance(fdeposit_details['time'], int) else fdeposit_details['time']
        description = 'fiat deposit activity'

        self.fee = float(fdeposit_details['fee'])
        self.feeAsset = asset if asset not in ['TUSD', 'USDC', 'USD', 'BUSD', 'USDP']  else 'USDT'
        self.id = fdeposit_details['id']
                
        super().__init__(asset, amount, time, description)

class DividendAction(ExchangeAccountAction):

    def __init__(self, dividend_details, re_key=True):

        if re_key:
            dividend_details = re_key_input(dividend_details)

        asset = dividend_details['asset']
        amount = float(dividend_details['amount'])
        time = timestampToString(dividend_details['time']) if isinstance(dividend_details['time'], int) else dividend_details['time']
        description = 'dividend payment'

        super().__init__(asset, amount, time, description)

class ConversionAction(ExchangeAccountAction):
    
    def __init__(self, conversion_details, re_key=True):

        fromAsset = conversion_details.get('fromAsset', None) if conversion_details.get('fromAsset', None) != None else conversion_details['base']
        
        if re_key:
            conversion_details = re_key_input(conversion_details)

        asset = conversion_details['asset']
        amount = float(conversion_details['amount'])
        time = timestampToString(conversion_details['time']) if isinstance(conversion_details['time'], int) else conversion_details['time']
        description = 'conversion action'

        self.base = fromAsset
        self.price = float(conversion_details['price'])

        super().__init__(asset, amount, time, description)

    def getOppositeLegAction(self):
        return ExchangeAccountAction(self.base, -1 * (self.price / self.amount), self.time, 'conversion activity')

class FeeAction(ExchangeAccountAction):

    def __init__(self, asset, amount, time, description):

        amount = amount * -1
        time = time
        
        super().__init__(asset, amount, time, description)