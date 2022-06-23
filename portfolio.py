from historical_data_manager import HistoricalDataManager
import datetime as dt
from exchange import Exchange

class Portfolio(object):

    def __init__(self, mongo_portfolio) -> None:
        self.mongo = mongo_portfolio
        self.reporting_currency = mongo_portfolio.portfolio_currency
        self.current_holdings = mongo_portfolio.current_holdings
        self.portfolio_nav = mongo_portfolio.portfolio_nav
        self.last_update_date = mongo_portfolio.last_update_date
        self.current_assets = mongo_portfolio.current_assets 
        self.current_holdings = mongo_portfolio.current_holdings
        self.current_allocation = mongo_portfolio.current_allocation
        self.exchanges = [Exchange.from_mongo(mongo_exchange) for mongo_exchange in mongo_portfolio.exchanges]
        self.cost_basis = {}
        
        # self.historical_data_manager = HistoricalDataManager(self.user_id, self.reporting_currency)

    def getDefaultExchange(self):
        for exchange in self.exchanges:
            if exchange.is_default:
                return exchange
        
        self.exchanges[0].is_default = 1
        return self.exchanges[0]

    # Portfolio update functions
    def update(self):
        # self.updateCostBasis()
        self.updateHoldings()
        self.updateNAV()
        self.updateAllocation()
        self.last_update_date = dt.datetime.utcnow()
        self.save()
        return
    
    def save(self):
        self.mongo.portfolio_nav      =  self.portfolio_nav
        self.mongo.current_assets     =  self.current_assets 
        self.mongo.current_holdings   =  self.current_holdings
        self.mongo.current_allocation =  self.current_allocation
        self.mongo.last_update_date   =  self.last_update_date
        self.mongo.save()

    def updateHoldings(self):
        current_holdings = {}
        for exchange in self.exchanges:
            exchange_holdings = exchange.getCurrentHoldings()
            for asset, amount in exchange_holdings.items():

                # Rename coins in Earn account to their actual names
                if asset in ['LDBTC', 'LDETH']:
                    asset = asset[2:]
               
                if current_holdings.get(asset, None) != None:
                    current_holdings[asset] += amount
                else:
                    current_holdings[asset] = amount
        
        self.current_assets = list(current_holdings.keys())
        self.current_holdings = current_holdings
    
    def updateNAV(self):
        # TODO(ion): Need to change this - we need to find the valid base currency e.g. (USDT vs BUSD vs USDC) plus account
        # for different currencies other than USD
        pref_exchange = self.getDefaultExchange()
        asset_prices = pref_exchange.getCurrentPriceForAssets(self.current_assets, self.reporting_currency)
        total = 0.0
        for asset in self.current_assets:
            total += (asset_prices[asset] * self.current_holdings[asset])
        
        self.portfolio_nav = total
    
    def updateAllocation(self):
        pref_exchange = self.getDefaultExchange()
        prices = pref_exchange.getCurrentPriceForAssets(self.current_assets, self.reporting_currency)
        allocation = {a_name: (prices[a_name] * a_holding) / self.portfolio_nav for a_name, a_holding in self.current_holdings.items()}
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