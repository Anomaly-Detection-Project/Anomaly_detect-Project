import pandas as pd
import os
from env import *

def df_to_csv(df):
    try:
        df.to_csv(filename, index=False, mode='x')
    except FileExistsError:
        df.to_csv(filename,index=False)

def manny_acquire(filename):
    exists = os.path.isfile(filename)
    if exists:
        df = pd.read_csv(filename)
        
        return df
    else:
        #Define query
        query = '''
                select * 
                from `logs`
                JOIN cohorts ON logs.cohort_id=cohorts.id;
                '''
        #Define url
        url = get_db_url('curriculum_logs')
        
        #Read data from SQL server
        df = pd.read_sql(query, url)
        
        #Cache
        df_to_csv(df) 
        
        return df

def manny_clean(df):
    # create datetime column and set as index
    df['fixed_date']=df['date']+ ' ' +df['time']
    df.set_index(pd.DatetimeIndex(df['fixed_date']), inplace=True)
   
    # drop unecessary columns after setting index
    df = df.drop(columns = ['date','time','fixed_date'])
   
    # creating columns for names of program_id
    df['data']= df['program_id']== 3
    df['web']= df['program_id']== 2
    df['php']= df['program_id']== 1
    df['front_end']= df['program_id']== 4
    
    # fix cohort_id float datatype to interger
    df['cohort_id'] = df['cohort_id'].astype(int)
    
    # fix 'to_date' column datatype to datetime
    df['end_date'] = pd.to_datetime(df['end_date'])
    
    # df = df[df['path'].str.len()>3]
    
    return df

def manny_wrangle(): 
    df = manny_acquire('curriculum_logs.csv')
    df = manny_clean(df) 
    return df


