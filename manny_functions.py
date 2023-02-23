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

def question_01():
    data_df = df[df['data']==True].copy()
    data_df = data_df[data_df['path']!= '/']
    top_results = data_df.groupby(['cohort_id', 'path'])['id'].count().reset_index().sort_values(['cohort_id', 'id'], ascending=[True, False]).groupby('cohort_id').nth(0)
    print(top_results)

def question_06():
    # preparing a subset dataframe 
    # filtered the logs by users who have already graduated
    grads_logs_df = df[df['end_date'].notnull()]

    # groupby() the logs by the graduating date for each user
    grad_date_by_user = grads_logs_df.groupby('user_id')['end_date'].max()
    
    # groupby() the filtered logs by path and user_id, and count the number of accesses
    topics_by_user = grads_logs_df.groupby(['path', 'user_id'])['id'].count()

    # function to check if a log row is after a user's graduating date
    def is_after_grad_date(row):
        user_id = row['user_id']
        log_date = row.name
        grad_date = grad_date_by_user[user_id]
        return log_date > grad_date

    # groupby() the logs by the graduating date for each user and count the number of accesses to each path by each user after their graduating date
    topics_by_user_after_grad_date = grads_logs_df[grads_logs_df.apply(is_after_grad_date, axis=1)].groupby(['path', 'user_id'])['id'].count()

    # groupby() the filtered logs by path, count the number of unique users who accessed each path after their graduating date, and sort the result
    topics_by_access = topics_by_user_after_grad_date.groupby('path').nunique().sort_values(ascending=False)

    # ouput
    print(topics_by_access)
