import pandas as pd
import tpqoa
import os
import re
import config

class OandaDataCollector():
    
    ''' Class for collection historical data from Oanda using API 
    
    Attrs
    ==================
    
    symbol - string
            ticker symbol e.g 'EUR_USD'
    
    start - string
            start date
    
    end - string
          end date
    
    period - string
           period in format e.g. 'M5' - 5 minutes, 'H1' - 1 hour
    
    
    spread_col - boolean
                add column with spreads
    
    '''
    def __init__(self, symbol, start, end, period='M1',
                  spread_col=False):
        self.symbol = symbol
        self.start = start
        self.end = end
        self.data = None
        self.period = period
        self.spread_col = spread_col
        self.spread_folder='/path/to/folder/spreads/'
        self.get_data()

    def __repr__(self):
        rep = "DataCollectorOanda(symbol = {}, start = {}, end = {}, period= {})"
        return rep.format(self.symbol, self.start, self.end, self.period)
    
    def add_spreads(self, df):
        
        '''Add columns with spreads and pips to df '''
        
        symbol = self.symbol.replace('_', '')
        spreads = pd.read_csv(f'{self.spread_folder}{symbol}_spreads.csv', parse_dates=['date'], index_col='date')
        df['weekday'] = df.index.day_name()
        df['hour'] = df.index.hour
        df = df[df['weekday'] != 'Sunday']
        m_df = pd.merge(df.reset_index(), spreads, how='left', on=['weekday', 'hour']).set_index('time').fillna(
            method='ffill')
        m_df = m_df.drop(['weekday', 'hour'], axis=1)
        return m_df
   
    def get_data(self):
        
        ''' Collect and prepares the data'''
        
        api = tpqoa.tpqoa("oanda.cfg")
        df = api.get_history(instrument=self.symbol, start=self.start, end=self.end,
                             granularity=self.period, price="M")
        
        if self.spread_col==True:
            df=self.add_spreads(df)
            
        df=df.reset_index().rename(columns={'time':'Date','o':'Open', 'h':'High', 'l':'Low',  'c':'Close', 'volume':'vol'}).set_index('Date')
        
        df=df.drop('complete', axis=1)
            
            
            
                

        self.data = df
