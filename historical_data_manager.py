
from tools import dbRead, HistoricalDataPackage

from pathlib import Path

class HistoricalDataManager(object):
    ''' 
        Database accessor only - will not update data
    '''

    def __init__(self, user_id, base_currency) -> None:
        self.user_id = user_id
        self.base_currency = base_currency
        self.historical_data_dir = Path('data/user_data/' + self.user_id + '/historical_data/')
        self.historical_market_data_dir = Path('data/market_data/')

    def getHistoricalHoldings(self, start_date, end_date, asset=None):
        historical_holdings = dbRead(self.historical_data_dir / 'historical_holdings.csv', start_date, end_date, asset)
        
        return historical_holdings
    
    def getHistoricalMarketData(self, start_date, end_date, freq, asset=None):
        
        if freq == 'daily':
            db_dir = self.historical_market_data_dir / 'daily_close.csv'
        elif freq == 'hourly':
            db_dir = self.historical_data_dir / 'hourly_close.csv'
        else:
            print('ERROR: Market data frequency {} not supported!'.format(freq))
            return HistoricalDataPackage([], {}, [])
        
        historical_market_data = dbRead(db_dir, start_date, end_date, asset, fill_values=1)
        
        if self.base_currency != 'USDT':
            fx_translation = dbRead(self.historical_market_data_dir, start_date, end_date, asset=self.base_currency, fill_values=1)
            historical_market_data = historical_market_data * fx_translation
        
        return historical_market_data

    def getHistoricalTrades(self, start_date, end_date, asset=None):
        db_dir = self.historical_data_dir / 'historical_trades.csv'
        historical_trades = dbRead(db_dir, start_date, end_date)
        
        if asset != None:
            for i,trade in enumerate(historical_trades.data[:]):
                if trade['asset'] != asset:
                    historical_trades.data.pop(i)

        return historical_trades

    
    def getHistoricalDustOperations(self, start_date, end_date):
        db_dir = self.historical_data_dir / 'historical_dust_activities.csv'
        historical_dust_activities = dbRead(db_dir, start_date, end_date)

        return historical_dust_activities