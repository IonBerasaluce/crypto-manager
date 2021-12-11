from pathlib import Path
from binance_manager import BinanceAccountManager
import pandas as pd
from exchange_actions import *

from utils import createProject

def genEntries(list_of_actions):
    list_of_dicts =  [action.toBaseDict() for action in list_of_actions]
    return pd.DataFrame(list_of_dicts).set_index('time')

class ExchangeManager(object):

    def __init__(self, my_coins) -> None:
        self._binance_manager = BinanceAccountManager(my_coins)
        self.my_coins = my_coins
        self.save_location = 'data/'

        if ~Path(self.save_location).is_file():
            createProject(self.save_location) 
    
    def getAllAccountMovements(self, start_date=None, end_date=None):
        
        trades = genEntries(self.getAccountTradeActions(start_date, end_date))
        deposits = genEntries(self.getAccountDepositActions(start_date, end_date))
        fiat = genEntries(self.getAccountFiatDepositsWithdrawalActions(start_date, end_date))
        withdrawals = genEntries(self.getAccountWithdrawalActions(start_date, end_date))
        dust = genEntries(self.getAccountDustActions(start_date, end_date))
        dividends = genEntries(self.getAccountDividendActions(start_date, end_date))

        all_moves = pd.concat([trades, deposits, fiat, withdrawals, dust, dividends], axis=0, sort=False).sort_index()

        return all_moves
    
    def getAccountTradeActions(self, start_date=None, end_date=None):
        trades = self._binance_manager.getTrades(s_date=start_date, e_date=end_date)
        trade_actions = [TradeAction(trade_action) for trade_action in trades]
        trade_fee_actions = [FeeAction(ta.feeAsset, ta.fee, ta.time, 'trading fees') for ta in trade_actions]
        trade_base_action = [ta.getOppositeLegAction() for ta in trade_actions]
        trade_actions.extend(trade_fee_actions + trade_base_action)
        return trade_actions

    def getAccountDepositActions(self, start_date=None, end_date=None):
        deposits = self._binance_manager.getDeposits(s_date=start_date)
        deposit_actions = [DepositAction(deposit_action) for deposit_action in deposits]
        return deposit_actions

    def getAccountWithdrawalActions(self, start_date=None, end_date=None):
        withdrawals = self._binance_manager.getWithdrawals(s_date=start_date)
        withdrawl_actions = [WithdrawlAction(withdrawl_action) for withdrawl_action in withdrawals]
        withdrawl_fee_actions = [FeeAction(wa.feeAsset, wa.fee, wa.time, 'withdrawl fees') for wa in withdrawl_actions]
        withdrawl_actions.extend(withdrawl_fee_actions)
        return withdrawl_actions

    def getAccountFiatDepositsWithdrawalActions(self, start_date=None, end_date=None):
        fiat = self._binance_manager.getFiatDepositsWithdrawals(s_date=start_date)
        fiat_actions = [FiatDepositAction(fiat_action) for fiat_action in fiat]
        fiat_fee_actions = [FeeAction(fa.feeAsset, fa.fee, fa.time, 'fiat deposit fee') for fa in fiat_actions]
        fiat_actions.extend(fiat_fee_actions)
        return fiat_actions

    def getAccountDustActions(self, start_date=None, end_date=None):
        dust = self._binance_manager.getAccountDust(s_date=start_date)
        dust_actions = [DustSweepAction(dust_action) for dust_action in dust]
        dust_transfer_actions = [da.getTransferAction() for da in dust_actions]
        dust_fee_actions = [FeeAction(da.feeAsset, da.fee, da.time, 'dust exchange fee') for da in dust_actions]
        dust_actions.extend(dust_fee_actions + dust_transfer_actions)
        return dust_actions
    
    def getAccountDividendActions(self, start_date=None, end_date=None):
        dividend = self._binance_manager.getAccountDividends(s_date=start_date)
        dividend_actions = [DividendAction(dividend_action) for dividend_action in dividend]
        return dividend_actions