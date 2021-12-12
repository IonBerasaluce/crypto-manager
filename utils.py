import datetime as dt
from numpy.lib.shape_base import _expand_dims_dispatcher
import pandas as pd

DIRS = ["historical_trades.csv", "historical_deposits.csv", "historical_withdrawals.csv", "historical_dust_actrivities.csv", "historical_fiat_movements.csv", "account_movements.csv"]


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
    return dt.datetime.fromtimestamp(int(timestamp/1000)).date()

def createProject(dir):
    for new_dir in DIRS:
        with open(dir + new_dir, "w") as empty_csv:
            pass


