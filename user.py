
from pandas.core.indexes import base
from global_vars import USER_DATA_PATH, EXCHANGE_CODES
from portfolio import Portfolio
from tools import createFileInDirectory, createJsonDescriptors
from exchange import Exchange
from update_user_data import createHistoricalUserData

import json
from pathlib import Path
import datetime as dt
import os

# When App starts
SETUP_DIR = Path('data/app_setup.json')
with open(SETUP_DIR) as infile:
    SETUP = json.load(infile)

DEFAULT_PORTFOLIO = {'portfolio_name': 'Untitled', 'update_date':'2017-01-01'}

class User(object):
    def __init__(self, survey) -> None:
        
        max_id = SETUP['total']
        all_users = User.getAllUsers()
        user_name = survey['user_name']

        if user_name in all_users:
            raise Exception('ERROR! - Username already in use please log in!')

        else:
            self.user_id = str(max_id + 1).zfill(4)
            self.user_name = user_name
            self.reporting_currency = survey['reporting_currency']
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
                "historical_holdings.csv",
                "historical_movements.csv",
                "historical_conversions.csv"
                ]
            
            base_directory = USER_DATA_PATH / (self.user_id + '/historical_data/')
            full_dirs = [base_directory / f for f in files_to_create]
            createFileInDirectory(full_dirs)
            createJsonDescriptors(base_directory)

            SETUP['total'] += 1
            self.save()
    
    @staticmethod
    def getAllUsers():
        return[SETUP['users'][user] for user in SETUP['users'].keys()]

    @classmethod
    def from_user_id(cls, id):

        ob = cls.__new__(cls)

        with open(SETUP_DIR) as infile:
            setup = json.load(infile)
        
        user_config = setup['users'][id]

        ob.user_id = id
        ob.user_name = user_config['user_name']
        ob.reporting_currency = user_config['reporting_currency']
        ob.user_exchanges = [Exchange.from_dict(exchange) for exchange in user_config['user_exchanges']]
        ob.user_portfolios = [Portfolio(port) for port in user_config['user_portfolios']]
        
        for port in ob.user_portfolios:
            port.addExchange(ob.user_exchanges)

        return ob

    def addExchange(self, exchange_data):
        exchange_code = EXCHANGE_CODES[exchange_data['exchange_name']]

        for exchange in self.user_exchanges:
            if exchange_code == exchange.code:
                print('Exchange {} already setup and ready to use!'.format(exchange_data['exchange_name']))
                return

        self.user_exchanges.append(Exchange(exchange_code, exchange_data['api_public'], exchange_data['api_secret'], exchange_data['is_default']))
    
        base_directory = USER_DATA_PATH / (self.user_id + '/historical_data/')

        for f in os.listdir(base_directory):
            if f.endswith(".json"):
                with open(base_directory / f) as infile:
                    my_dict = json.load(infile)
                
                file_name = f.split('.')[0]

                if file_name == "historical_movements" or file_name == "historical_holdings":
                    continue
                elif file_name  == 'historical_trades':
                    my_dict.update({exchange_code:{"last_update_date": "", "assets_traded":[]}})
                else:
                    my_dict.update({exchange_code:{"last_update_date": ""}})

                with open(base_directory / f, 'w') as outfile:
                    json.dump(my_dict, outfile, indent=4)

        self.updatePortfolios()
        self.save()
        
        createHistoricalUserData(self.user_id, self.user_exchanges[-1])
        return
    
    def getExchanges(self):
        return [exchange.name for exchange in self.user_exchanges]


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
            elif k == 'user_exchanges':
                serial_exchanges = []
                for exchange in v:
                    serial_exchanges.append(exchange.toDict())
                outdict.update({'user_exchanges': serial_exchanges})
            else:
                outdict.update({k:v})

        return outdict
    
    def createPortfolio(self, port_settings):
        port_settings['user_id'] = self.user_id
        port_settings['reporting_currency'] = self.reporting_currency
        port_settings['current_holdings'] = {}
        port_settings['current_NAV'] = 0.0
        port_settings['update_date'] = ''
        port_settings['cost_basis'] = {}
        port_settings['current_allocation'] = {}
        port_settings['current_assets'] = []

        portfolio = Portfolio(port_settings)

        exchange_names = port_settings.get('exchanges', [])
        if exchange_names:
            for name in exchange_names:
                exchanges_to_add = next((exchange for exchange in self.user_exchanges if exchange.name == name), None)
        else:
            exchanges_to_add = self.user_exchanges
        
        portfolio.addExchange(exchanges_to_add)
        portfolio.update()
        self.user_portfolios.append(portfolio)

    #TODO(ion): Add a force update parameter here which needs to be called upon adding a new Exchange to the portfolio
    def updatePortfolios(self):

        if not self.user_portfolios:
            self.createPortfolio(DEFAULT_PORTFOLIO)
            return

        date = dt.datetime.utcnow()

        for port in self.user_portfolios:
            update_date = dt.datetime.strptime(port.update_date, "%Y-%m-%d %H:%M:%S")
            update_diff = (date - update_date).total_seconds() / 60.0

            if update_diff > 30:
                port.update()
        
        return