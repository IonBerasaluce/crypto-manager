from historical_data_manager import HistoricalDataManager
from exchange import Exchange
import datetime as dt

class Portfolio(object):

    def __init__(self, port_settings) -> None:
        self.portfolio_name = port_settings['portfolio_name']
        self.reporting_currency = port_settings['reporting_currency']
        self.current_holdings = port_settings['current_holdings']
        self.current_NAV = port_settings['current_NAV'],
        self.update_date = port_settings['update_date']
        self.user_id = port_settings['user_id']
        self.current_assets = port_settings['current_assets']
        self.current_allocation = port_settings['current_allocation']
        self.exchanges = []
        self.cost_basis = port_settings['cost_basis']
        self.historical_data_manager = HistoricalDataManager(self.user_id, self.reporting_currency)

    # Portfolio util Functions
    def toDict(self):
        dict_out = {k:v for k,v in self.__dict__.items() if k not in ['exchanges', 'historical_data_manager']}
        return dict_out

    def addExchange(self, exchanges):
        for exchange in exchanges:
            if isinstance(exchange, dict):
                ex = Exchange(exchange['code'], exchange['public_key'], exchange['secret_key'], exchange['is_default'])
            else:
                ex = exchange
            self.exchanges.append(ex)
        
    def getDefaultExchange(self):
        for exchange in self.exchanges:
            if exchange.is_default:
                return exchange
        
        self.exchanges[0].is_default = 1
        return self.exchanges[0]
    
    def rename(self, name):
        self.portfolio_name = name

    # Portfolio update functions
    def update(self):
        # self.updateCostBasis()
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

    # TODO(ion): Rethink this function - issue when loading the account for the first time
    def updateCostBasis(self):
        start_date = self.update_date
        end_date = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        new_trades = self.historical_data_manager.getHistoricalTrades(start_date, end_date)

        if new_trades.data:
            for trade in new_trades.data:
                asset = trade['asset']
                if trade['type'] == 'Buy':
                    current_cost = self.cost_basis.get(asset, 0.0)
                    # Watch out here as this must include all of our accounts
                    current_holding = self.current_holdings.get(asset, 0.0)
                    self.cost_basis[asset] = ((current_cost * current_holding) + trade['amount'] * trade['price'] * trade['basePrice']) / (current_holding + trade['amount']) 
                else:
                    self.cost_basis[asset] = self.cost_basis.get(trade['asset'], 0.0)

        new_dust_activities = self.historical_data_manager.getHistoricalDustOperations(start_date, end_date)
        
        if new_dust_activities.data:
            for activity in new_dust_activities.data:
                asset = activity['transferedAsset']
                current_cost = self.cost_basis.get(asset, 0)
                current_holding = self.current_holdings.get(asset, 0.0)
                self.cost_basis[asset] = ((current_cost * current_holding) + activity['transferedAmount'] * activity['basePrice']) / (current_holding + activity['transferedAmount'])

        return

    def getHistoricalHoldings(self, start_date, end_date):
        return self.historical_data_manager.getHistoricalHoldings(start_date, end_date)
    
    def getHistoricalNAV(self, start_date=None, end_date=None):
        historical_holdings = self.historical_data_manager.getHistoricalHoldings(start_date, end_date)
        historical_prices = self.historical_data_manager.getHistoricalMarketData(start_date, end_date, 'daily', asset=historical_holdings.assets)
        
        total_NAV = [0] * len(historical_holdings.dates)
        for i, holdings in enumerate(historical_holdings.data):
            prices = historical_prices.data[i]
            notional_by_holding = {k: prices.get(k, 0)*v for k, v in holdings.items()}
            
            for asset in historical_holdings.assets:
                total_NAV[i] += notional_by_holding[asset]

        return {'dates': historical_holdings.dates, 'notional': total_NAV}