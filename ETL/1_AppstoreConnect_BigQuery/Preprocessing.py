#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
class EmptyDate(Exception):
    def __init__(self,text):
        print(text)
        
class EmptyString(Exception):
    def __init__(self,text):
        print(text)
        
def check_csv(table):
    if (((table['Event_Date'] == ' ') | (table['Event_Date'].isnull())).sum() != 0):
        raise EmptyDate('Колонка с `Event Date` без даты')
    if (table["Standard_Subscription_Duration"].isnull().sum() != 0 ):
        raise EmptyString('Недостает значение в `Standard Subscription Duration`')
def match_duration(row,durations):
        duration = row['Standard Subscription Duration']
        sub_id = row['Subscription Apple ID']
        if duration is np.NaN:
            duration = durations.query('`Subscription Apple ID` == @sub_id')['Standard Subscription Duration'].values[0]
        return duration  
class Preprocessing:
    def __init__(self, path):
        self.path = path
        
    def is_missed_values(self,df,column):
            return True if ((((df[column] == ' ')).sum() != 0 | (df[column].isnull())).sum() != 0) else False
        
    def is_str_boolean(self,df,column):
        return True if ( ((df[column] == 'Yes').sum() != 0) | ((df[column] == 'No').sum() != 0)) else False
    
    def  event_date_fill(self,df):
        if self.is_missed_values(df,'Event Date'):
            date = df['Event Date'].value_counts(sort=True).index[0] # самая популярная дата
            # Замена ' '
            df['Event Date'] = df['Event Date'].str.replace(' ',date)
            # Замена nan
            df.fillna({'Event Date': date},inplace=True)
            df['Event Date'] = pd.to_datetime(df['Event Date'],format='%Y-%m-%d').dt.date
        return df    
    
    def duration_fill(self,df):
        if self.is_missed_values(df,'Standard Subscription Duration'):
            durations = df[['Subscription Apple ID','Standard Subscription Duration']].value_counts().index.to_frame(index=False)
            df['Standard Subscription Duration'] = df.apply(match_duration,durations=durations,axis=1)
        return df
    def subscriber_fill(self,df):
            if self.is_missed_values(df,'Subscriber ID'):
                df['Subscriber ID'] = df['Subscriber ID'].str.replace(' ','111')
                df['Subscriber ID'] = df['Subscriber ID'].astype(np.int64)
            return df
    # 1
    def refund_fill(self,df):
        df['Refund'] = df['Refund'].map({'Yes': True,np.nan:False}) 
        return df
    # 2
    def pricing_fill(self,df):
        if self.is_str_boolean(df,'Preserved Pricing'):
                df['Preserved Pricing'] = df['Preserved Pricing'].map({'Yes': True}) 
                df['Preserved Pricing'] = df['Preserved Pricing'].astype('bool')
        return df
    def get_data(self):
        df = pd.read_csv(self.path, sep='\t', header=0,parse_dates=['Event Date'])
        df = self.event_date_fill(df)
        df = self.duration_fill(df)
        df = df.replace(' ',np.nan)
        if 'Subscriber ID' in df.columns:
            df = self.refund_fill(df)    
            df['Customer Price'] = df['Customer Price'].astype(np.float64)
            df['Subscriber ID'] = df['Subscriber ID'].astype('Int64')
        else:  
            df['Original Start Date'] = pd.to_datetime(df['Original Start Date'],format='%Y-%m-%d')
            df = self.pricing_fill(df)
            df['Subscription Apple ID'] = df['Subscription Apple ID'].astype('Int64')
            df['Client'] = df['Client'].astype(str)
            df['Days Before Canceling'] = df['Days Before Canceling'].astype('Int64')
            df['Days Canceled'] = df['Days Canceled'].astype('Int64')
            df['Previous Subscription Apple ID'] = df['Previous Subscription Apple ID'].astype('Int64')
        df['Event Date'] = pd.to_datetime(df['Event Date'],format='%Y-%m-%d')
        df = df.replace(' ',np.nan)
        df.columns = df.columns.str.replace(' ','_')
        return df

