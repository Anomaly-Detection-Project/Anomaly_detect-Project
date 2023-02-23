#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os
from env import host, username, password


def prep_curr_logs(df):
    '''Prepares acquired curriculum_logs data for exploration'''
    
    # combines the date and time columns into one column
    df['fixed_date'] = df.date + ' ' + df.time
    
    # sets the fixed_date column to a datetime format and sets this column as the index
    df.fixed_date = pd.to_datetime(df.fixed_date)
    df = df.set_index(df.fixed_date)
    
    # dropping unnecessary and duplicate columns
    df = df.drop(columns=['date', 'time', 'id', 'slack', 'deleted_at'])
    
    # adding columns for specific codeup programs
    df['data']= df['program_id']== 3
    df['web']= df['program_id']== 2
    df['php']= df['program_id']== 1
    df['front_end']= df['program_id']= 4
    
    # adding a column is_active that shows if the log in was attempted when the user was active in the program
    active = ([df['end_date'] >= df['fixed_date']])
    df['is_active'] = active[0]
    
    # Convert binary categorical variables to numeric
    df['is_active'] = df.is_active.map({True: 1, False: 0})
    
    return df

