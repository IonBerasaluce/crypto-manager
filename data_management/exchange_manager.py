from binance_manager import BinanceAccountManager
import pandas as pd

from exchange_actions import *
from utils import readCSV

def genEntries(list_of_actions, mode='base'):
    if list_of_actions:
        
        if mode == 'base':
            list_of_dicts =  [action.toBaseDict() for action in list_of_actions]
        else:
            list_of_dicts =  [action.toDict() for action in list_of_actions]
        
        return pd.DataFrame(list_of_dicts).set_index('time')

    else:
        return pd.DataFrame()

class ExchangeManager(object):

    def __init__(self, my_coins, location) -> None:
        self._binance_manager = BinanceAccountManager(my_coins)
        self.my_coins = my_coins
        self.save_location = location

    def updateDBs(self, db, start_date, end_date):
        
        file_to_update = readCSV(self.save_location / db, index=None, as_type=str)
        
        if not file_to_update.empty:
            file_to_update = file_to_update.set_index('time')
        
        print('----------------')
        print('Updating: {}'.format(db))
        print('----------------')
        
        if db == 'historical_deposits.csv':
            deposits = self.getAccountDeposits(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, deposits], axis=0).sort_index()
        elif db == 'historical_dust_activities.csv':
            dust = self.getAccountDust(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, dust], axis=0).sort_index()
        elif db == 'historical_fiat_movements.csv':
            fiat = self.getAccountFiatDepositsWithdrawals(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, fiat], axis=0).sort_index()
        elif db == 'historical_trades.csv':
            trades = self.getAccountTrades(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, trades], axis=0).sort_index()
        elif db == 'historical_withdrawals.csv':
            withdrawals = self.getAccountWithdrawals(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, withdrawals], axis=0).sort_index()
        elif db == 'historical_dividends.csv':
            dividends = self.getAccountDividends(start_date, end_date, export_mode='full')
            file_to_update = pd.concat([file_to_update, dividends], axis=0).sort_index()

        file_to_update.to_csv(self.save_location / db)
        
        print('{} db update complete!'.format(db))

        return  

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

    def getAdditionalInfo(self, actions):
        if type(actions[0]) == TradeAction:
            trade_fee_actions = [FeeAction(ta.feeAsset, ta.fee, ta.time, 'trading fees') for ta in actions]
            trade_base_action = [ta.getOppositeLegAction() for ta in actions]
            actions.extend(trade_fee_actions + trade_base_action)
        elif type(actions[0]) == FiatDepositAction:
            fiat_fee_actions = [FeeAction(fa.feeAsset, fa.fee, fa.time, 'fiat deposit fee') for fa in actions]
            actions.extend(fiat_fee_actions)
        elif type(actions[0]) == WithdrawlAction:
            withdrawl_fee_actions = [FeeAction(wa.feeAsset, wa.fee, wa.time, 'withdrawl fees') for wa in actions]
            actions.extend(withdrawl_fee_actions)
        elif type(actions[0]) == DustSweepAction:
            dust_transfer_actions = [da.getTransferAction() for da in actions]
            dust_fee_actions = [FeeAction(da.feeAsset, da.fee, da.time, 'dust exchange fee') for da in actions]
            actions.extend(dust_fee_actions + dust_transfer_actions)
        elif type(actions[0] == ConversionAction):
            conversion_base_action = [cv.getOppositeLegAction() for cv in actions]
            actions.extend(conversion_base_action)
        else:
            print('No additional actions for Exchange Action type: {}'.format(type(actions[0])))

        return actions

    def _getAccountTradeActions(self, start_date=None, end_date=None, additional_info=False):
        trades = self._binance_manager.getTrades(s_date=start_date, e_date=end_date)
        trade_actions = [TradeAction(trade_action) for trade_action in trades]
        if additional_info:
            trade_actions = self.getAdditionalInfo(trade_actions)
        return trade_actions

    def _getAccountDepositActions(self, start_date=None, end_date=None):
        deposits = self._binance_manager.getDeposits(s_date=start_date, e_date=end_date)
        deposit_actions = [DepositAction(deposit_action) for deposit_action in deposits]
        return deposit_actions

    def _getAccountWithdrawalActions(self, start_date=None, end_date=None, additional_info=False):
        withdrawals = self._binance_manager.getWithdrawals(s_date=start_date, e_date=end_date)
        withdrawl_actions = [WithdrawlAction(withdrawl_action) for withdrawl_action in withdrawals]
        if additional_info:
            withdrawl_actions = self.getAdditionalInfo(withdrawl_actions)
        return withdrawl_actions

    def _getAccountFiatDepositsWithdrawalActions(self, start_date=None, end_date=None, additional_info=False):
        fiat = self._binance_manager.getFiatDepositsWithdrawals(s_date=start_date, e_date=end_date)
        fiat_actions = [FiatDepositAction(fiat_action) for fiat_action in fiat]
        if additional_info:
            fiat_actions = self.getAdditionalInfo(fiat_actions)
        return fiat_actions

    def _getAccountDustActions(self, start_date=None, end_date=None, additional_info=False):
        dust = self._binance_manager.getAccountDust(s_date=start_date, e_date=end_date)
        dust_actions = [DustSweepAction(dust_action) for dust_action in dust]
        if additional_info:
            dust_actions = self.getAdditionalInfo(dust_actions)
        return dust_actions
    
    def _getAccountDividendActions(self, start_date=None, end_date=None):
        dividend = self._binance_manager.getAccountDividends(s_date=start_date, e_date=end_date)
        dividend_actions = [DividendAction(dividend_action) for dividend_action in dividend]
        return dividend_actions