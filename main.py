
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

from user import User

exchange_inputs = {
        'exchange_name': 'Binance', 
        'api_public': 'RsUPwFY1b3X9w7aWh5Ix55TsIZbgHBjxmS5lwUazpju1tKvlCYFK8V1HlsZ8eLkK', 
        'api_secret': 'CXxctMRkQmRBIs0AmHReGV2iEd6QlQ0zuz3FlnjLz3qfn05WTC0EyrLdAIpE6BCj',
        'is_default': 1
    }

survey = { "user_name": "ion.berasaluce16@gmail.com",
           "reporting_currency": "USDT"
         }

user1 = User.from_user_id('0001')
user1.addExchange(exchange_inputs)
historical_NAV = user1.getHistoricalNAV('2021-03-01', '2021-11-30')
user1.save()

import matplotlib.pyplot as plt

plt.plot(historical_NAV['dates'], historical_NAV['notional'])
plt.show()


# portfolio.historicalNAV
# portfolio.historicalWeights
# portfolio.currentAllocation
# portfolio.currentNAV
# portfolio.currentAssets

# portfolio.getTaxInfo()
# portfolio.recentMovements
# portfolio.totalFees

