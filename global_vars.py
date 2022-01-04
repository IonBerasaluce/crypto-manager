from pathlib import Path

BASECURR = 'USDT'
USER_DATA_PATH = Path('data/user_data')
MARKET_DATA_PATH = Path('data/market_data')
SETUP_DIR = Path('data/app_setup.json')

BINANCE_KEY_MAP = {
    'coin':                   'asset',
    'amount':                 'amount',
    'insertTime':             'time',
    'network':                'network',
    'address':                'address',
    'status':                 'status',
    'txId':                   'id',
    'qty':                    'amount',
    'time':                   'time',
    'isBuyer':                'isBuyer',
    'symbol':                 'symbol',
    'price':                  'price',
    'commission':             'fee',
    'commissionAsset':        'feeAsset',
    'id':                     'id',
    'applyTime':              'time',
    'transactionFee':         'fee',
    'fromAsset':              'asset',
    'operateTime':            'time',
    'serviceChargeAmount':    'fee',
    'transferedAmount':       'transferedAmount',
    'transId':                'id',
    'fiatCurrency':           'asset',
    'createTime':             'time',
    'totalFee':               'fee',
    'orderNo':                'id',
    'divTime':                'time',
    'asset':                  'asset',
    'toAmount':               'amount',
    'fromAmount':             'amount_sold',
    'ratio':                  'price',
    'toAsset':                'asset'
    }