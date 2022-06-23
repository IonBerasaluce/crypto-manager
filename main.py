
# User config gives us the exchanges that the user is setup with
# User id from when the user first got into the platform which links to the data
# 

'''
Goals:
    1. Must work seamlesly with binance (i.e. no need for csv or manual corrections )
    2. Must allow for upload of a csv files from any other main exchange 
    (Coinbase, FTX, Kraken, Gemini, Crypto.com)
    3. Handle a user base of approx 10 people - before scaling
    4. Aside from the obvious below we can add performance by crypto


For the historical databases, for each db save a json file that gives details on:
    1. The assets that are saved in the db
    2. The update date of the db 
    3. The start-date and end-date of each asset in the db

This will avoid us from having to load this information

'''

import datetime as dt
from infrastructure.switchlang import switch
from colorama import Fore
from passlib.hash import pbkdf2_sha256

import data.mongo_setup as mongo_setup
import services.data_service as svc
import infrastructure.state as state
from update_user_data_new import loadAllDataFromExchange
from portfolio import Portfolio

def show_commands():
    print('What action would you like to take:')
    print('[V]iew your portfolios')
    print('[C]reate a new portfolio')
    print('Edit [P]ortfolio')
    print('Setup a new [E]xchange')
    print('Add Offline [W]allet')
    print('[H]ome page')
    print('e[X]it')

def show_portfolio_commands():
    print('Update reporting [C]urrency')
    print('[R]ename Portfolio')
    print('[H]ome page')

def print_header():
    print('----------------------------------------------')
    print('|                                             |')
    print('|               CFOIL v.01                    |')
    print('|             console edition                 |')
    print('|                                             |')
    print('----------------------------------------------')
    print()

def config_mongo():
    mongo_setup.global_init()

def main():
    print_header()
    config_mongo()
    user_loop()

def create_account():
    print('============= Register =============')
    username = input('Input your username: ')
    email = input('Input your email address: ').strip().lower()
    password = pbkdf2_sha256.hash(input('Enter you password: '))

    acc = svc.find_account_by_email(email)
    if acc:
        print('Error: Account with email: {} already exists'.format(email))
        return

    state.active_user = svc.create_account(username, email, password)    
    print("Created new account with id {}".format(state.active_user.id))    
    home_page()

def login():
    print('============= Login =============')
    email = input('Enter your email address? ').strip().lower()
    password = input('Enter your password: ')
    
    user = svc.find_account_by_email(email)
    if not user:
        print("Could not find account with email {}".format(email))
        return
    
    if not pbkdf2_sha256.verify(password, user.user_password):
        print("Incorrect Username/Password please try again")
        login()
    
    state.active_user = user
    home_page()

def create_portfolio():
    print('============= Create a Portfolio =============')

    if not state.active_user:
        print("You must login to continue")
        return
    
    name = input("Portfolio name: ")
    currency = input("Portfolio base currency: ")
    portfolio = svc.create_portfolio(state.active_user, name, currency)
    state.reload_account()

    setup_exchange(portfolio)

    print('Successfully added Portfolio: {} to your account'.format(portfolio.portfolio_name))

def edit_portfolio():
    print('============= Edit a Portfolio =============')
    show_portfolio_commands()

    action = get_action()

    with switch(action) as s:
        s.case('c', change_portfolio_currency)
        s.case('h', home_page)
        s.case('x', exit_app)
        s.default(unknown_command)


def add_exchange():
    print('============= Add Exchange =============')
    if not state.active_user:
        print("You must login to continue")
    
    port_name = input("Please input the portfolio name you would like to add the exchange to: ")
    user_portfolios = svc.get_user_portfolios(state.active_user, name=port_name)
    
    for portfolio in user_portfolios:
        setup_exchange(portfolio)

def setup_exchange(portfolio):
    if not state.active_user:
        print("You must login to add an exchange")
        return

    print("Enter the required exchange data: ")
    exchange_name = input("Exchange name: ")
    exchange_pkey = input("Public Key: ")
    exchange_skey = input("Secret Key: ")

    exchange = svc.add_exchange(portfolio, exchange_name, exchange_pkey, exchange_skey)

    # loadAllDataFromExchange(portfolio, exchange)

    print("Successfully added: {} Exchange to your portfolio: {}!".format(exchange.exchange_name, portfolio.portfolio_name))

def view_portfolios():

    if not state.active_user:
        print("You must login to view your porfolios")
        return
    
    user = state.active_user
    user_portfolios = svc.get_user_portfolios(user)

    if not user_portfolios:
        print(" There are no portoflios associated to your account. [c]reate a portfolio?")
        return

    for mongo_portfolio in user_portfolios:

        portfolio = Portfolio(mongo_portfolio)

        if (mongo_portfolio.last_update_date - dt.datetime.today()).seconds > 30*60:
            portfolio.update()

        print("Current NAV in {}: ".format(mongo_portfolio.portfolio_name))
        print(str(portfolio.portfolio_nav) + ' ' + portfolio.reporting_currency)
        print("Current holdings in {}: ".format(mongo_portfolio.portfolio_name))
        print(portfolio.current_holdings)
        print("Current allocation in portfolio {}".format(mongo_portfolio.portfolio_name))
        print(portfolio.current_allocation)

def change_portfolio_currency():
    
    if not state.active_user:
        print("You must login to update your portfolios")

    user = state.active_user
    user_portfolios = svc.get_user_portfolios(user)

    for port in user_portfolios:
        print(port.portfolio_name)
    
    target_portfolio_name = input("Please select the portfolio you would like to change the reporting currency: ")
    
    for port in user_portfolios:
        if port.portfolio_name == target_portfolio_name:
            portfolio_to_change = port
        else:
            print("Portfolio {} not found, please select a portfolio from the list below: ".format(target_portfolio_name))
            change_portfolio_currency()
    
    new_reporting_currency = input("Please input the new reporting currency: ")
    portfolio_to_change.portfolio_currency = new_reporting_currency

    portfolio_to_change.save()

    print("Successfuly changed the reporting currency of portfolio: {} to {}".format(target_portfolio_name, new_reporting_currency))

def add_offline_wallet():
    print("TODO: Ion still needs to implement this")

def user_loop():

    print("Available actions:")
    print(" * [G]et Started")
    print(" * [L]ogin")
    print(" * E[X]it")
    print()

    while True:
        action = get_action()
        
        with switch(action) as s:
            s.case('g', create_account)
            s.case('l', login)
            s.case('v', view_portfolios)
            s.case('p', edit_portfolio)
            s.case('e', add_exchange)
            s.case('c', create_portfolio)
            s.case('h', home_page)
            s.case('w', add_offline_wallet)
            s.case('x', exit_app)
            s.default(unknown_command)

def home_page():
    user = state.active_user
    print("Welcome {}!".format(user.user_name))
    show_commands()

def unknown_command():
    print("Sorry command not found.")

def exit_app():
    user = state.active_user
    print()
    if user:
        print('User {} logged off!'.format(user.user_name))
    print('Goodbye!')
    raise KeyboardInterrupt()

def get_action():
    text = '> '
    if state.active_user:
        text = '{}> '.format(state.active_user.user_name)
    action = input(Fore.YELLOW + text + Fore.WHITE)
    return action.strip().lower()

if __name__ == '__main__':
    main()
    

# portfolio.historicalNAV
# portfolio.historicalWeights
# portfolio.currentAllocation
# portfolio.currentNAV
# portfolio.currentAssets

# portfolio.getTaxInfo()
# portfolio.recentMovements
# portfolio.totalFees

