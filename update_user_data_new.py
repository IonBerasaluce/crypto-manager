import datetime as dt
from exchange import Exchange

def loadAllDataFromExchange(portfolio, mongo_exchange):
    end_date = dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    start_date = mongo_exchange.last_update_date.strftime('%Y-%m-%d %H:%M:%S')
    assets_traded = mongo_exchange.assets_traded
    
    exchange = Exchange(mongo_exchange.exchange_code, mongo_exchange.exchange_pkey, mongo_exchange.exchange_skey, True)

    new_trades = exchange.getTrades(start_date, end_date, assets_traded)
    new_deposits = exchange.getDeposits(start_date, end_date)
    new_withdrawals = exchange.getWithdrawals(start_date, end_date)
    new_fiat = exchange.getFiatTransactions(start_date, end_date)
    new_dust = exchange.getAccountDust(start_date, end_date)
    new_dividends = exchange.getAccountDividends(start_date, end_date)
    new_conversions = exchange.getAccountConversions(start_date, end_date)

    # Save the data in the database
    portfolio.add_historical_trades(new_trades)
    portfolio.add_historical_deposits(new_deposits)
    portfolio.add_historical_withdrawals(new_withdrawals)
    portfolio.add_historical_fiat_transactions(new_fiat)
    portfolio.add_historical_dust_actions(new_dust)
    portfolio.add_historical_dividends(new_dividends)
    portfolio.add_historical_conversions(new_conversions)

    # After the first account load (assets_traded was an empty list because we just added the exchange)
    # we save down the assets that were traded throughout time in that exchange
    if not assets_traded:
        assets_traded = list(set([trade['asset'] for trade in new_trades]))



