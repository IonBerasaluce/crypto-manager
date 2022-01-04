
from pandas.core.indexes import base
from global_vars import USER_DATA_PATH
from portfolio import Portfolio
from tools import createFileInDirectory, createJsonDescriptors
from exchange import EXCHANGE_CODES
from update_user_data import createUserData

import json
from pathlib import Path
import datetime as dt
import os

# When App starts
SETUP_DIR = Path('data/app_setup.json')
with open(SETUP_DIR) as infile:
    SETUP = json.load(infile)


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
            
            if update_diff > 0:
                port.updatePortfolio()

        return ob

    def addExchange(self, exchange_data):
        exchange_code = EXCHANGE_CODES[exchange_data['exchange_name']]
        self.user_exchanges.append({'code': exchange_code, 'public_key': exchange_data['api_public'], 'secret_key': exchange_data['api_secret'], 'is_default':exchange_data['is_default']})
        self.save()
    
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

        createUserData(self.user_id, self.user_exchanges[-1])
        return

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
        port_settings['cost_basis'] = {}
        port_settings['current_allocation'] = {}
        port_settings['current_assets'] = []

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