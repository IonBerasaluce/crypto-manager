import datetime as dt
import pandas as pd
import os
from pathlib import Path

DIRS =  ["data/exchange_data/historical_trades.csv", 
         "data/exchange_data/historical_deposits.csv", 
         "data/exchange_data/historical_withdrawals.csv", 
         "data/exchange_data/historical_dust_activities.csv", 
         "data/exchange_data/historical_fiat_movements.csv", 
         "data/exchange_data/historical_dividends.csv", 
         "data/derived_data/account_movements.csv"]

def toTimeStamp(date):
    return int(dt.datetime.timestamp(date)*1000)

def toDate(timestamp):
    return dt.datetime.fromtimestamp(int(timestamp/1000))

def gen_90d_dates(start_date, end_date=None):
    dates = []
    s_date = toDate(start_date)
    if end_date == None:
        final_date = dt.datetime.today()
    else:
        final_date = toDate(end_date)
    while(True):
        e_date = s_date + dt.timedelta(days=90)
        if e_date > final_date:
            dates.append((toTimeStamp(s_date), toTimeStamp(final_date)))
            break
        else:
            dates.append((toTimeStamp(s_date), toTimeStamp(e_date)))
            s_date = s_date + dt.timedelta(days=90 + 1)

    return dates

def readCSV(path, index=0, as_type=None):
    #TODO(ion): THis function needs to be re-written the conditions dont line up correctly
    try: 
        if as_type == None:
            data = pd.read_csv(path, index_col=index)
        else:
            data = pd.read_csv(path, index_col=index, dtype=as_type)
            #TODO(ion) Remove this hardcode
            data.time = data.time.astype('int64')
        
        if index != None:
            data.index = pd.to_datetime(data.index)
    
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    
    return data


def createProject(path):
    paths = [os.path.join(path, directory) for directory in DIRS]
    createFolder(paths)

def mapper(fnc):
    def inner(list_of_directories):
        
        return [fnc(directory) for directory in list_of_directories]
    
    return inner
    
@mapper
def createFolder(directory):
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



