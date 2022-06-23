from typing import List

from data.mongo_user import User
from data.mongo_portfolio import MongoPortfolio
from data.mongo_exchange import MongoExchange

def find_account_by_email(email: str) -> User:
    user = User.objects(user_email=email).first()
    return user

def create_account(username: str, email: str, password: str) -> User:
    user = User()
    user.user_name = username
    user.user_email = email
    user.user_password = password

    user.save()

    return user

def create_portfolio(active_user: User,
                     name: str,
                     currency: str, 
                     ) -> MongoPortfolio:

    portfolio = MongoPortfolio()
    portfolio.portfolio_name = name
    portfolio.portfolio_currency = currency

    #Update the other portfolio fields here before saving I guess...
    portfolio.save()

    user = find_account_by_email(active_user.user_email)
    user.portfolio_ids.append(portfolio.id)
    user.save()

    return portfolio

def add_exchange(portfolio: MongoPortfolio, 
                 name: str, 
                 pkey: str, 
                 skey: str) -> MongoExchange:
    
    exchange = MongoExchange()
    exchange.exchange_name = name
    exchange.exchange_pkey = pkey
    exchange.exchange_skey = skey

    if name == 'Binance':
        exchange.exchange_code = 'e0001'

    portfolio.exchanges.append(exchange)

    portfolio.save()

    return exchange

def get_user_portfolios(active_user: User, name='') -> List[MongoPortfolio]:
    if name == '':
        portfolios = MongoPortfolio.objects(id__in=active_user.portfolio_ids)
    else:
        portfolios = MongoPortfolio.objects(id__in=active_user.portfolio_ids, portfolio_name=name)
    
    return portfolios

