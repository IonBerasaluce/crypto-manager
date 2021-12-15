
from utils import readCSV

class DBRequest(object):
    def __init__(self, asset=None, start_time=None, end_time=None):
        self.asset = asset
        self.start_time = start_time
        self.end_time = end_time
        self.exchange_db_directory = 'data/exchange_data/'
        self.derived_db_directory = 'data/derived_data/'

    def toDict(self):
        return_dict = {}
        for k,v in self.__dict__.iteritems():
            if v == None:
                continue
            else:
                return_dict.update({k:v})

    def processRequest(self, db):
        #TODO(Ion): Here we have an issue with index=None and user supplying dates
        try:
            df = readCSV(db, index=None, as_type=str)
            df.index = df['time']        
            if self.asset == None:
                req = df.loc[self.start_time:self.end_time, :]
            else:
                req = df.loc[df['asset'] == self.asset].loc[self.start_time:self.end_time]

            records = req.to_dict('records')
            return records
        
        except Exception as e:
            print(e)
            return []
    
    def getTrades(self):
        db = self.exchange_db_directory + 'historical_trades.csv'
        return self.processRequest(db)

    def getDeposits(self):
        db = self.exchange_db_directory + 'historical_deposits.csv'
        return self.processRequest(db)

    def getWithdrawals(self):
        db = self.exchange_db_directory + 'historical_withdrawals.csv'
        return self.processRequest(db)

    def getDividends(self):
        db = self.exchange_db_directory + 'historical_dividends.csv'
        return self.processRequest(db)

    def getFiatActivities(self):
        db = self.exchange_db_directory + 'historical_fiat_movements.csv'
        return self.processRequest(db)

    def getDustActivities(self):
        db = self.exchange_db_directory + 'historical_dust_activities.csv'
        return self.processRequest(db)

    def getConversionMovements(self):
        db = self.exchange_db_directory + 'historical_conversions.csv'
        return self.processRequest(db)

    def getAccountMovements(self):
        db = self.derived_db_directory + 'account_movements.csv'
        return self.processRequest(db)