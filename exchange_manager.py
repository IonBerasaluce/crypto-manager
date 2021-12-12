from os import stat_result
from pathlib import Path
from binance_manager import BinanceAccountManager
import pandas as pd

from exchange_actions import *
from utils import createProject, readCSV

def genEntries(list_of_actions, mode='base'):
    if mode == 'base':
        list_of_dicts =  [action.toBaseDict() for action in list_of_actions]
    else:
        list_of_dicts =  [action.toDict() for action in list_of_actions]
    return pd.DataFrame(list_of_dicts).set_index('time')

class ExchangeManager(object):

    def __init__(self, my_coins, location) -> None:
        self._binance_manager = BinanceAccountManager(my_coins)
        self.my_coins = my_coins
        self.save_location = location

        if ~Path(self.save_location).is_file():
            createProject(self.save_location)

    def updateDBs(self, db, start_date, end_date):
        
        file_to_update = readCSV(self.save_location + db)
        
        print('----------------')
        print('Updating: {}'.format(db))
        print('----------------')
        
        if db == 'account_movements.csv':
            all_moves = self.getAllAccountMovements(start_date, end_date) 
            file_to_update = pd.concat([file_to_update, all_moves], axis=0).sort_index()
        if db == 'historical_deposits.csv':
            deposits = self.getAccountDeposits(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, deposits], axis=0).sort_index()
        if db == 'historical_dust_deposits.csv':
            dust = self.getAccountDust(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, dust], axis=0).sort_index()
        if db == 'historical_fiat_movements.csv':
            fiat = self.getAccountFiatDepositsWithdrawals(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, fiat], axis=0).sort_index()
        if db == 'historical_trades.csv':
            trades = self.getAccountTrades(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, trades], axis=0).sort_index()
        if db == 'historical_withdrawals.csv':
            withdrawals = self.getAccountWithdrawals(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, withdrawals], axis=0).sort_index()
        if db == 'historical_dividends.csv':
            dividends = self.getAccountDividends(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, dividends], axis=0).sort_index()

        file_to_update.to_csv(self.save_location + db)
        
        print('{} db update complete!'.format(db))

        return  

    def getAllAccountMovements(self, start_date=None, end_date=None):
        
        # Must change this to update once the data has been downlaoded and saved locally
        trades = self.getAccountTrades(start_date, end_date, additional_info=True)
        deposits = self.getAccountDeposits(start_date, end_date)
        fiat = self.getAccountFiatDepositsWithdrawals(start_date, end_date, additional_info=True)
        withdrawals = self.getAccountWithdrawals(start_date, end_date, additional_info=True)
        dust = self.getAccountDust(start_date, end_date, additional_info=True)
        dividends = self.getAccountDividends(start_date, end_date)

        all_moves = pd.concat([trades, deposits, fiat, withdrawals, dust, dividends], axis=0, sort=False).sort_index()

        return all_moves

    def getAccountDeposits(self, start_date, end_date, export_mode='base'):
        return genEntries(self._getAccountDepositActions(start_date, end_date), export_mode)
        
    def getAccountDust(self, start_date, end_date, additional_info=False, export_mode='base'):
        return genEntries(self._getAccountDustActions(start_date, end_date, additional_info), export_mode)
        
    def getAccountFiatDepositsWithdrawals(self, start_date, end_date, additional_info=False, export_mode='base'):
        return genEntries(self._getAccountFiatDepositsWithdrawalActions(start_date, end_date, additional_info), export_mode)
        
    def getAccountTrades(self, start_date, end_date, additional_info=False, export_mode='base'):
        return genEntries(self._getAccountTradeActions(start_date, end_date, additional_info), export_mode)
        
    def getAccountWithdrawals(self, start_date, end_date, additional_info=False, export_mode='base'):
        return genEntries(self._getAccountWithdrawalActions(start_date, end_date, additional_info), export_mode)
        
    def getAccountDividends(self, start_date, end_date, export_mode='base'):
        return genEntries(self._getAccountDividendActions(start_date, end_date), export_mode)


    def _getAccountTradeActions(self, start_date=None, end_date=None, additional_info=False):
        trades = self._binance_manager.getTrades(s_date=start_date, e_date=end_date)
        trade_actions = [TradeAction(trade_action) for trade_action in trades]
        if additional_info:
            trade_fee_actions = [FeeAction(ta.feeAsset, ta.fee, ta.time, 'trading fees') for ta in trade_actions]
            trade_base_action = [ta.getOppositeLegAction() for ta in trade_actions]
            trade_actions.extend(trade_fee_actions + trade_base_action)
        return trade_actions

    def _getAccountDepositActions(self, start_date=None, end_date=None):
        deposits = self._binance_manager.getDeposits(s_date=start_date, e_date=end_date)
        deposit_actions = [DepositAction(deposit_action) for deposit_action in deposits]
        return deposit_actions

    def _getAccountWithdrawalActions(self, start_date=None, end_date=None, additional_info=False):
        withdrawals = self._binance_manager.getWithdrawals(s_date=start_date, e_date=end_date)
        withdrawl_actions = [WithdrawlAction(withdrawl_action) for withdrawl_action in withdrawals]
        if additional_info:
            withdrawl_fee_actions = [FeeAction(wa.feeAsset, wa.fee, wa.time, 'withdrawl fees') for wa in withdrawl_actions]
            withdrawl_actions.extend(withdrawl_fee_actions)
        return withdrawl_actions

    def _getAccountFiatDepositsWithdrawalActions(self, start_date=None, end_date=None, additional_info=False):
        fiat = self._binance_manager.getFiatDepositsWithdrawals(s_date=start_date, e_date=end_date)
        fiat_actions = [FiatDepositAction(fiat_action) for fiat_action in fiat]
        if additional_info:
            fiat_fee_actions = [FeeAction(fa.feeAsset, fa.fee, fa.time, 'fiat deposit fee') for fa in fiat_actions]
            fiat_actions.extend(fiat_fee_actions)
        return fiat_actions

    def _getAccountDustActions(self, start_date=None, end_date=None, additional_info=False):
        dust = self._binance_manager.getAccountDust(s_date=start_date, e_date=end_date)
        dust_actions = [DustSweepAction(dust_action) for dust_action in dust]
        if additional_info:
            dust_transfer_actions = [da.getTransferAction() for da in dust_actions]
            dust_fee_actions = [FeeAction(da.feeAsset, da.fee, da.time, 'dust exchange fee') for da in dust_actions]
            dust_actions.extend(dust_fee_actions + dust_transfer_actions)
        return dust_actions
    
    def _getAccountDividendActions(self, start_date=None, end_date=None):
        dividend = self._binance_manager.getAccountDividends(s_date=start_date, e_date=end_date)
        dividend_actions = [DividendAction(dividend_action) for dividend_action in dividend]
        return dividend_actions