
# User config gives us the exchanges that the user is setup with
# User id from when the user first got into the platform which links to the data
# 

'''
Goals:
    1. Must work seamlesly with binance (i.e. no need for csv or manual corrections )
    2. Must allow for upload of a csv files from any other main exchange 
    (Coinbase, FTX, Kraken, Gemini, Crypto.com)
    3. Handle a user base of approx 10 people - before scaling
    4. Aside from the obvious below we can add performance by crypto


For the historical databases, for each db save a json file that gives details on:
    1. The assets that are saved in the db
    2. The update date of the db 
    3. The start-date and end-date of each asset in the db

This will avoid us from having to load this information

'''

import datetime as dt
from pathlib import Path
import json
import os
from typing import Dict
from binance.client import Client
from pandas.core import base

# When App starts
SETUP_DIR = Path('data/app_setup.json')
with open(SETUP_DIR) as infile:
    SETUP = json.load(infile)

EXCHANGE_CODES = {
    'Binance': 'e0001',
    'Coinbase': 'e0002',
    'Kraken':  'e0003'
}

def mapper(fnc):
    def inner(list_of_directories):
        
        return [fnc(directory) for directory in list_of_directories]
    
    return inner
    
@mapper
def createFileInDirectory(directory):
    try:
        if not os.path.exists(directory):
            # Here we must create the directory first and then create the folder
            out_folder = Path(*Path(directory).parts[:-1])
            if not out_folder.is_dir():
                os.makedirs(out_folder)
            with open(directory, 'w') as newfile:
                pass
    except OSError as e:
        print('ERROR {}: Creating directory {}. '.format(e, directory))

class HistoricalDataPackage(object):
    def __init__ (self, dates, data, assets):
        self.dates = dates
        self.data = data
        self.assets = assets

def dbRead(db_name, start_date, end_date, assets=None):
    import pandas as pd
    directory = Path(db_name)
    start_date, end_date = pd.to_datetime([start_date, end_date])

    data = pd.read_csv(directory, index_col=0)
    if assets != None:
        data = data.reindex(assets, axis=1)

    data.index = pd.to_datetime(data.index)
    data = data.loc[start_date:end_date, :]
    
    dates = list(data.index)
    assets = list(data.columns)
    out_data = data.to_dict('records')
    
    data_packet = HistoricalDataPackage(dates, out_data, assets)
    return data_packet

class HistoricalDataManager(object):
    def __init__(self, user_id, base_currency) -> None:
        self.user_id = user_id
        self.base_currency = base_currency
        self.historical_data_dir = Path('data/user_data/' + self.user_id + '/historical_data/')
        self.historical_market_data_dir = Path('data/market_data/daily_market_data.csv')

    def getHistoricalHoldings(self, start_date, end_date, asset=None):
        historical_holdings = dbRead(self.historical_data_dir / 'historical_holdings.csv', start_date, end_date, asset)
        
        return historical_holdings
    
    def getHistoricalMarketData(self, start_date, end_date, asset=None):
        historical_market_data = dbRead(self.historical_market_data_dir, start_date, end_date, asset)
        
        if self.base_currency != 'USDT':
            fx_translation = dbRead(self.historical_market_data_dir, start_date, end_date, asset=self.base_currency)
        
        historical_market_data = historical_market_data * fx_translation
        return historical_market_data

class Portfolio(object):

    def __init__(self, port_settings) -> None:
        self.portfolio_name = port_settings['portfolio_name']
        self.reporting_currency = port_settings['reporting_currency']
        self.current_holdings = port_settings['current_holdings']
        self.current_NAV = port_settings['current_NAV'],
        self.update_date = port_settings['update_date']
        self.user_id = port_settings['user_id']
        self.historical_data_manager = HistoricalDataManager(self.user_id, self.reporting_currency)
        self.current_assets = []
        self.current_allocation = {}
        self.exchanges = []
        self.cost_basis = {}

    # Portfolio util Functions
    def toDict(self):
        dict_out = {k:v for k,v in self.__dict__.items() if k not in ['exchanges', 'historical_data_manager']}
        return dict_out

    def addExchange(self, exchanges):
        for exchange in exchanges:
            ex = Exchange(exchange['code'], exchange['public_key'], exchange['secret_key'], exchange['is_default'])
            self.exchanges.append(ex)
        
    def getDefaultExchange(self):
        for exchange in self.exchanges:
            if exchange.is_default:
                return exchange
        
        self.exchanges[0].is_default = 1
        return self.exchanges[0]

    # Portfolio update functions
    def updatePortfolio(self):
        self.updateHoldings()
        self.updateNAV()
        self.updateAllocation()
        self.update_date = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        return
    
    def updateHoldings(self):
        current_holdings = {}
        for exchange in self.exchanges:
            exchange_holdings = exchange.getCurrentHoldings()
            for asset, amount in exchange_holdings.items():
               
                if current_holdings.get(asset, None) != None:
                    current_holdings[asset] += amount
                else:
                    current_holdings[asset] = amount
        
        self.current_assets = list(current_holdings.keys())
        self.current_holdings = current_holdings
    
    def updateNAV(self):
        pref_exchange = self.getDefaultExchange()
        asset_prices = pref_exchange.getCurrentPriceForAssets(self.current_assets, self.reporting_currency)
        total = 0.0
        for asset in self.current_assets:
            total += (asset_prices[asset] * self.current_holdings[asset])
        
        self.current_NAV = total
    
    def updateAllocation(self):
        pref_exchange = self.getDefaultExchange()
        prices = pref_exchange.getCurrentPriceForAssets(self.current_assets, self.reporting_currency)
        allocation = {a_name: (prices[a_name] * a_holding) / self.current_NAV for a_name, a_holding in self.current_holdings.items()}
        self.current_allocation = allocation

    def getHistoricalHoldings(self, start_date, end_date):
        return self.historical_data_manager.getHistoricalHoldings(start_date, end_date)
    
    def getHistoricalNAV(self, start_date=None, end_date=None):
        historical_holdings = self.historical_data_manager.getHistoricalHoldings(start_date, end_date)
        historical_prices = self.historical_data_manager.getHistoricalMarketData(start_date, end_date, self.reporting_currency)
        
        total_NAV = [0] * len(historical_holdings['dates'])
        for asset in historical_holdings['holdings'].keys():
            asset_historical_notional = [price * amount for price, amount in zip(historical_prices['assets'][asset], historical_holdings['holdings'][asset])]
            total_NAV = [x + y for x, y in zip(total_NAV, asset_historical_notional)]

        return {'dates': historical_holdings['dates'], 'notional': total_NAV} 
    
class Exchange(object):

    def __init__(self, code, pkey, skey, is_default) -> None:
        if code == 'e0001':
            self.exchange = BinanceExchange(pkey, skey)   
            self.is_default = is_default         
        else:
            exchange_name = {v:k for k, v in EXCHANGE_CODES.items}[code]
            raise Exception('Support for {} Exchange is comming soon!'.format(exchange_name))

    @classmethod
    def from_dict(cls, exchange_setup):
        return cls(exchange_setup['code'], exchange_setup['public_key'], exchange_setup['secret_key'])

    def getCurrentHoldings(self):
        return self.exchange.getCurrentHoldings()
    
    def getCurrentPriceForAssets(self, assets, base_currency):
        assets = [asset if asset != 'BUSD' else 'USDT' for asset in assets ]
        symbols = [asset + base_currency for asset in assets if asset != base_currency]
        prices = self.exchange.getCurrentPrice(symbols)
        out_dict = {k.split(base_currency)[0]: v for k,v in prices.items()}
        out_dict.update({base_currency: 1.0})
        return out_dict

class BinanceExchange(object):

    def __init__(self, public_key, secret_key) -> None:
        self.client = Client(public_key, secret_key)

    def getCurrentHoldings(self):
        balances = self.client.get_account()['balances']
        holdings = {}
        for asset in balances:
            asset_amount = float(asset['free']) + float(asset['locked'])
            if asset_amount > 0:
                holdings[asset['asset']] = asset_amount

        return holdings
    
    def getCurrentPrice(self, symbols) -> Dict:
        prices = {}
        for symbol in symbols:
            prices[symbol] = float(self.client.get_avg_price(symbol=symbol)['price'])
        
        return prices

class User(object):
    def __init__(self, survey) -> None:
        
        max_id = SETUP['total']
        all_users = User.getAllUsers()

        self.user_id = str(max_id + 1).zfill(4)
        user_name = survey['user_name']

        if user_name in all_users:
            raise Exception('ERROR! - Username already in use please log in!')

        else:
            self.user_name = user_name
            self.user_portfolios = []
            self.user_exchanges = []

            # Must create all the data files for the user here
            files_to_create = [ 
                "historical_trades.csv", 
                "historical_deposits.csv", 
                "historical_withdrawals.csv", 
                "historical_dust_activities.csv", 
                "historical_fiat_movements.csv", 
                "historical_dividends.csv", 
                "account_movements.csv"
                ]
            
            base_directory = 'data/user_data/' + self.user_id + '/historical_data/'
            full_dirs = [base_directory + f for f in files_to_create]
            createFileInDirectory(full_dirs)

            SETUP['total'] += 1
            self.save()
    
    @staticmethod
    def getAllUsers():
        return[SETUP['users'][user] for user in SETUP['users'].keys()]

    @classmethod
    def from_user_id(cls, id):

        ob = cls.__new__(cls)
        date = dt.datetime.utcnow()

        with open(SETUP_DIR) as infile:
            setup = json.load(infile)
        
        user_config = setup['users'][id]

        ob.user_id = id
        ob.user_name = user_config['user_name']
        ob.user_exchanges = user_config['user_exchanges']
        ob.user_portfolios = [Portfolio(port) for port in user_config['user_portfolios']]

        for port in ob.user_portfolios:
            port.addExchange(ob.user_exchanges)
            update_date = dt.datetime.strptime(port.update_date, "%Y-%m-%d %H:%M:%S")
            update_diff = (date - update_date).total_seconds() / 60.0
            
            if update_diff > 30.0:
                port.updatePortfolio()

        return ob

    def addExchange(self, exchange_data):
        exchange_code = EXCHANGE_CODES[exchange_data['exchange_name']]
        self.user_exchanges.append({'code': exchange_code, 'public_key': exchange_data['api_public'], 'secret_key': exchange_data['api_secret'], 'is_default':exchange_data['is_default']})
    
    def getUserPortfolio(self, portfolio_name):
        for portfolio in self.user_portfolios:
            if portfolio.portfolio_name == portfolio_name:
                return portfolio
            else:
                raise Warning('A portfolio named {} was not found please try again!'.format(portfolio_name))

    def save(self):
        existing_ids = SETUP['users'].keys()
        
        if self.user_id in existing_ids:
            SETUP['users'][self.user_id] = self.toDict()
        else:
            SETUP['users'].update({self.user_id: self.toDict()})

        with open(SETUP_DIR, 'w') as outfile:
            json.dump(SETUP, outfile, indent=4)
        
        return
    
    def deleteUser(self):
        SETUP['users'].pop(self.user_id, None)
        with open(SETUP_DIR, 'w') as outfile:
            json.dump(SETUP, outfile, indent=4)

    def toDict(self):
        outdict = {}
        for k,v in self.__dict__.items():
            if k == 'user_portfolios':
                serial_ports = []
                for port in v:
                    serial_ports.append(port.toDict())
                outdict.update({'user_portfolios': serial_ports})
            else:
                outdict.update({k:v})

        return outdict
    
    def createPortfolio(self, port_settings):
        port_settings['user_id'] = self.user_id
        port_settings['current_holdings'] = {}
        port_settings['current_NAV'] = 0.0
        port_settings['update_date'] = ''
        portfolio = Portfolio(port_settings)
        portfolio.addExchange(self.user_exchanges)
        portfolio.updatePortfolio()
        self.user_portfolios.append(portfolio)

    def updateCurrentHoldings(self):
        for portfolio in self.user_portfolios:
            portfolio.updateHoldings()

    def getCurrentHoldings(self, force_update=False):
        output = {}

        if force_update:
            self.updateCurrentHoldings()

        for portfolio in self.user_portfolios:
            
            if not portfolio.current_holdings:
                self.updateCurrentHoldings()

            output[portfolio.portfolio_name] = portfolio.current_holdings
        
        return output


user1 = User.from_user_id('0001')
portfolio = user1.getUserPortfolio('My First Portfolio')
historical_holdings = portfolio.getHistoricalHoldings('2021-11-01', '2021-11-30')
user1.save()

import matplotlib.pyplot as plt
import pandas as pd

plt.plot(historical_holdings.dates, pd.DataFrame(historical_holdings.data))
plt.show()

# exchange_inputs = {
#         'exchange_name': 'Binance', 
#         'api_public': 'RsUPwFY1b3X9w7aWh5Ix55TsIZbgHBjxmS5lwUazpju1tKvlCYFK8V1HlsZ8eLkK', 
#         'api_secret': 'CXxctMRkQmRBIs0AmHReGV2iEd6QlQ0zuz3FlnjLz3qfn05WTC0EyrLdAIpE6BCj',
#         'is_default': 1
#     }

# port_settings = {
#     'portfolio_name': 'MyFirstPortfolio',
#     'reporting_currency': 'USDT'
# }

# port_settings = {
#     'portfolio_name': 'MyFirstPortfolio',
#     'reporting_currency': 'USDT'
# }

# survey = { "user_name": 'javibera2601@gmail.com'}




# portfolio.historicalNAV
# portfolio.historicalWeights
# portfolio.currentAllocation
# portfolio.currentNAV
# portfolio.currentAssets

# portfolio.getTaxInfo()
# portfolio.recentMovements
# portfolio.totalFees



# user_db.saveUserPortfolio(user1, portfolio)

# In the background - we need to update the market data db 
