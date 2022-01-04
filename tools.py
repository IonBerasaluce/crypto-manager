
from pathlib import Path
import datetime as dt
import os
import json
import csv
import pandas as pd

 

class HistoricalDataPackage(object):
    def __init__ (self, dates, data, assets):
        self.dates = dates
        self.data = data
        self.assets = assets

def mapper(fnc):
    def inner(list_of_directories):
        
        return [fnc(directory) for directory in list_of_directories]
    
    return inner
    
@mapper
def createFileInDirectory(directory):
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

def createJsonDescriptors(directory):

    files_to_create = [
        "historical_trades.json", 
        "historical_deposits.json", 
        "historical_withdrawals.json",
        "historical_dust_activities.json", 
        "historical_fiat_movements.json", 
        "historical_dividends.json", 
        "historical_movements.json",  
        "historical_holdings.json",
        "historical_conversions.json"
    ]

    for f in files_to_create:
        
        file_name = f.split('.')[0]
        
        if file_name == 'historical_trades':
            out_dict = {"headers": ["time","symbol","price","base",
                                    "basePrice","fee","feeAsset","id","feeAssetPrice",
                                    "asset","amount","description","type"]}
        
        elif file_name == 'historical_deposits':
            out_dict = {"headers": ["time","network","address","status","id","asset",
                                    "amount","description"]}

        elif file_name == 'historical_withdrawals':
            out_dict = {"headers": ["time","network","address","status","fee",
                                    "feeAsset","feeAssetPrice","id","asset",
                                    "amount","description"]}

        elif file_name == 'historical_dust_activities':
            out_dict = {"headers": ["time","fee","feeAsset","feeAssetPrice",
                                    "transferedAmount","transferedAsset","id",
                                    "asset","amount","description"]}

        elif file_name == 'historical_fiat_movements':
            out_dict = {"headers": ["time","fee","feeAsset","feeAssetPrice",
                                    "id","asset","amount","description"]}

        elif file_name == 'historical_dividends':
            out_dict = {"headers": ["time","asset","amount","description"]}
        
        elif file_name == 'historical_conversions':
            out_dict = {"headers":['time', 'price', 'asset', 'base', 'amount', 'description']}

        elif file_name == 'historical_holdings':
            out_dict = {"last_update_date": "", "headers":[]}
        
        elif file_name == 'historical_movements':
            out_dict = {"last_update_date": "", "headers": ["time","asset","amount",
                                                            "description"]}

        with open(directory / f, 'w') as outfile:
            json.dump(out_dict, outfile, indent=4)
        


def toTimeStamp(date):
    return int(dt.datetime.timestamp(date)*1000)

def toDate(timestamp):
    return dt.datetime.fromtimestamp(int(timestamp/1000))

def dateToString(date):
    return date.strftime('%Y-%m-%d %H:%M:%S')

def stringToDate(date):
    return dt.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

def timestampToString(ts):
    return dateToString(toDate(ts))

def stringToTimeStamp(date):
    return toTimeStamp(stringToDate(date))


def dbRead(db_name, start_date, end_date, combine_dates=False, headers=[], fill_values=0, filters=[]):
    directory = Path(db_name).with_suffix('.csv')

    data = pd.read_csv(directory, index_col=0)
    
    if filters:
        for filt in filters:
            data = data[data[[*filt][0]] == filt[[*filt][0]]]

    if headers:
        data = data.reindex(headers, axis=1)

    start_date = start_date if start_date > data.index[0] else data.index[0]    
    end_date = end_date if end_date < data.index[-1] else data.index[-1]

    data = data.loc[start_date:end_date, :].fillna(fill_values)
    
    if combine_dates:
        data['time'] = data.index

    assets = list(data.columns)
    out_data = data.to_dict('records')
    
    data_packet = HistoricalDataPackage(list(data.index), out_data, assets)
    return data_packet

def constructHistoricalHoldingsFromActions(actions):

    now_date = dateToString(dt.datetime.utcnow())[:10]
    
    actioned_assets = list(set([action.asset for action in actions]))
    all_actions_dicts = [action.toBaseDict() for action in actions]

    all_actions_df = pd.DataFrame(all_actions_dicts).set_index('time')
    all_actions_df['date'] = all_actions_df.index.str.slice(0,10)
    dates = list(all_actions_df['date']) + [now_date]
    
    new_index = pd.to_datetime(pd.unique(dates))
    historical_positioning = pd.DataFrame(data=0.0, index = new_index, columns=actioned_assets).resample('D').bfill()
    historical_positioning.index = historical_positioning.index.strftime("%Y-%m-%d")
    
    for asset in actioned_assets:
        coin_movements = all_actions_df[all_actions_df['asset']==asset].groupby('date').sum().rename({'amount': asset}, axis=1)
        historical_positioning.loc[coin_movements.index, asset] = coin_movements[asset]
    
    historical_positioning = historical_positioning.cumsum()
    historical_positioning['time'] = historical_positioning.index

    out_dates = list(historical_positioning.index)
    out_dict = historical_positioning.to_dict('records')
    out_assets = list(historical_positioning.columns)
    
    return HistoricalDataPackage(out_dates, out_dict, out_assets)

def gen_date_pairs(start_date, end_date=None, freq=90, out_type='string'):
    '''
    start_date: str
    end_date: str

    return list of out_type
    '''
    dates = []
    s_date = stringToDate(start_date)

    if end_date == None:
        final_date = dt.datetime.today()
    else:
        final_date = stringToDate(end_date)
    while(True):
        e_date = s_date + dt.timedelta(days=freq)
        if e_date > final_date:
            dates.append((s_date, final_date))
            break
        else:
            dates.append((s_date, e_date))
            s_date = s_date + dt.timedelta(days=freq + 1)

    if out_type == 'timestamp':
        dates = [(toTimeStamp(a_date), toTimeStamp(b_date)) for a_date, b_date in dates]
    else:
        print('Unsupported output type {}, date values returned as strings'.format(out_type))
        dates = [(dateToString(a_date), dateToString(b_date)) for a_date, b_date in dates]
    
    return dates

def getDBInfo(db):
    db = db.with_suffix('.json')
    with open(db) as infile:
        info = json.load(infile)
    return info

def updateDBInfo(db, out_dict):
    db = db.with_suffix('.json')
    with open(db, 'w') as outfile:
        json.dump(out_dict, outfile, indent=4)

def addRowsToDB(db, rows, headers):
    db = db.with_suffix('.csv')
    rows = sorted(rows, key=lambda d: d['time']) 
    with open(db, 'a', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, headers)
        dict_writer.writerows(rows)
    return

def dumpToDB(db, data, headers):
    db = db.with_suffix('.csv')
    data = sorted(data, key=lambda d: d['time']) 
    with open(db, 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, headers)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    
    print('Dumped data to db {}!'.format(db))
    return