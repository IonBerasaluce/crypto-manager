import datetime as dt
import pandas as pd

def gen_90d_dates(date):
    dates = []
    s_date = date

    while(True):
        e_date = s_date + dt.timedelta(days=90)
        if e_date > dt.datetime.today():
            dates.append((s_date, dt.datetime.today()))
            break
        else:
            dates.append((s_date, e_date))
            s_date = s_date + dt.timedelta(days=90 + 1)

    return dates

def readCSV(path):
    data = pd.read_csv(path, index_col=0)
    data.index = pd.to_datetime(data.index)
    return data
    