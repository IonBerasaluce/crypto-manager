import datetime as dt
import pandas as pd
import os

DIRS = ["historical_trades.csv", "historical_deposits.csv", "historical_withdrawals.csv", "historical_dust_activities.csv", "historical_fiat_movements.csv", "account_movements.csv", "historical_dividends.csv"]

def gen_90d_dates(start_date, end_date=None):
    dates = []
    s_date = start_date
    if end_date == None:
        final_date = dt.datetime.today()
    else:
        final_date = end_date
    while(True):
        e_date = s_date + dt.timedelta(days=90)
        if e_date > final_date:
            dates.append((s_date, final_date))
            break
        else:
            dates.append((s_date, e_date))
            s_date = s_date + dt.timedelta(days=90 + 1)

    return dates

def readCSV(path):
    try: 
        data = pd.read_csv(path, index_col=0)
        data.index = pd.to_datetime(data.index)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    
    return data

def toTimeStamp(date):
    return int(dt.datetime.timestamp(date)*1000)

def toDate(timestamp):
    return dt.datetime.fromtimestamp(int(timestamp/1000))

def createProject(path):
    paths = [path + directory for directory in DIRS]
    createFolder(paths)

def mapper(fnc):
    def inner(list_of_directories):
        
        return [fnc(directory) for directory in list_of_directories]
    
    return inner
    
@mapper
def createFolder(directory):
    try:
        if not os.path.exists(directory):
            with open(directory, 'w') as newfile:
                pass
    except OSError:
        print('ERROR: Creating directory. ' + directory)



