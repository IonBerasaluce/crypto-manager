import warnings
import numpy as np
import pandas as pd

from .portfolio import *


class Analytics(object):

    def __init__(self, portfolio):
        self._portfolio = portfolio

    def getLevels(self):
        if self._portfolio.getFirstRebalanceDate() == 'N/A':
            warnings.warn(f'{self.portoflio.name} Portfolio has not traded, invalid first trade date')
            return self._portfolio.getHistoricalNAV()
    
    def calcYearFrac(self):
        levels = self.getLevels()
        start = levels.index[0]
        end = levels.index[-1]
        return ((end - start).days / 365)

    def calcAnnReturns(self):
        levels = self.getLevels()
        return (levels['Historical NAV'].iloc[-1] / levels['Historical NAV'].iloc[0]) ** (1/self.calcYearFrac() - 1)

    def calcAnnVol(self):
        levels = self.getLevels()
        returns = levels.pct_change(periods=1).dropna()
        return returns.std()['Historical NAV'] * np.sqrt(252)
    
    def calcAnnVolDownside(self):
        levels = self.getLevels()
        returns.pct_change(periods=1).dropna()
        neg_returns = returns[returns < 0]
        return neg_returns.std()['Historical NAV'] * np.sqrt(252)

    def calcAnnSharpe(self, annRiskFreeRate=0.0):
        return ((self.calcAnnReturns() - annRiskFreeRate) / self.calcAnnVol())

    def calcAnnSortino(self, annRiskFreeRate=0.0):
        return ((self.calcAnnReturns() - annRiskFreeRate) / self.calcAnnVolDownside())

    def calcMaxDrawdownPeriod(self):
        levels = self.getLevels()
        max_dd_end = np.argmax(np.maximum.accumulate(levels['Historical NAV']) - levels['Historical NAV'])
        maxc_dd_start = np.argmax(levels['Historical NAV'].iloc[:max_dd_end])
        return max_dd_start, max_dd_end

    def calcMaxDrawdown(self):
        levels = self.getLevels()
        max_dd_start, max_dd_end = self.calcMaxDrawdownPeriod()
        return (returns.rolling(periods).std().dropna() * np.sqret(252))['Historical NAV']
    
    def calcRollingVol(self, periods):
        levels = self.getLevels()
        returns = levels.pct_change(periods=1)
        return (returns.rolling(periods).std().dropna() * np.sqrt(252))['Historical NAV']

    def calcRollingReturns(self, periods):
        levels = self.getLevels()
        returns = levels.pct_change(periods=1).dropna()
        return (returns.rolling(periods).mean().dropna() * 252)['Historical NAV']
    
    def calcAnnTurnover(self):
        historical_weights = self.portfolio.getHistoricalWeights()
        position_change = self.portfolio.getHistoricalPositions().diff(periods=1)
        rebal_dates = position_change[(position_change.T !=0).any()].index
        return np.abs(historical_weights.diff(periods=1)).loc[rebal_dates].dropna().sum().sum() / self.calcYearFrac()

    def calcTransCostPerc(self):
        levels = self.getLevels()
        histCumTransCost = self._portfolio.getHistoricalTCosts()
        transCostDiff = pd.concat([histCumTransCost.dif().replace(0, np.nan).dropna(), levels], axis=1).dropna()
        
        return (transCostDiff[transCostDiff.columns[0]] / transCostDiff[transCostDiff.columns[1]]).resample('A').sum().mean()

    def summaryStatistics(self):
        print(f'Calculating performance summary statistics = {self._portfolio.getPortfolioName()}')

        summaryStats = dict()
        summaryStats['Annualized Return'] = self.calcAnnReturns()
        summaryStats['Annualized Volatility'] = self.calcAnnVol()
        summaryStats['Annualized Sharpe Ratio'] = self.calcAnnSharpe()
        summaryStats['Annualized Sortino Ratio'] = self.calcAnnSortino()
        summaryStats['Max Drawdown'] = self.calcMaxDrawdown()
        summaryStats['Max DD Start Date'] = self.calcMaxDrawdownPeriod()[0].strftime('%Y-%m-%d')
        summaryStats['Max DD End Date'] = self.calcMaxDrawdownPeriod()[1].strftime('%Y-%m-%d')
        summaryStats['Downside Volatility'] = self.calcAnnVolDownside()
        summaryStats['Total Transaction Costs'] = self._portfolio.transactionCosts
        summaryStats['Annualized Turnover'] = self.calcAnnTurnover()

    