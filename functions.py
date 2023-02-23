#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd

def q_one(df):
    # preparing a subset dataframe of just Data Science
    data_df = df[df['data']==True].copy()
   
    # removing rows in 'path' column having '/'
    data_df = data_df[data_df['path']!= '/']
    
    # groupby() cohort & path combination and counting, sorted by count of 'id'
    # used a 2nd groupby using .nth method to get only the top count of 'id' for each group of cohorts
    top_results = data_df.groupby(['cohort_id', 'path'])['id'].count().reset_index().sort_values(['cohort_id', 'id'], ascending=       [True, False]).groupby('cohort_id').nth(0)
    
    # show results
    print(top_results)

def q_two(df):
    '''
    This function creates a new df that produces three columns and a count of 
    most accessed lesson plans by cohort.
    '''

    weird_df= df.groupby(['cohort_id', 'path']).agg('count')
    weird_df.sort_values(by= 'user_id', ascending= False)
    weird_df= weird_df.reset_index()
    weird_df= weird_df[weird_df.path != '/']
    count_df= weird_df[['cohort_id', 'path', 'ip']]
    count_df.sort_values(by= ['cohort_id', 'ip'], ascending= [False, False])
    new_df= count_df.sort_values(by= 'ip', ascending= False).groupby('cohort_id').nth(0)
    new_df.rename(columns= {'ip': 'count'}, inplace= True)
    return new_df

def q_three(df):
    '''
    This function creates a new df that produces two columns with only active user
    log in attempts and a count of least active users.
    '''
    
    df1 = df.loc[df["is_active"] ==1 ]
    weird_df2= df1.groupby(['user_id']).agg('count')
    weird_df2.sort_values(by= 'cohort_id', ascending= False)
    weird_df2= weird_df2.reset_index()
    weird_df2= weird_df2[weird_df2.ip < 50]
    count_df2= weird_df2[['user_id', 'ip']]
    count_df2.sort_values(by= ['user_id', 'ip'], ascending= [False, False])
    new_df2= count_df2.sort_values(by= 'ip', ascending= False).groupby('user_id').nth(0)
    new_df2 = new_df2.sort_values('ip')
    return new_df2


def q_seven(df):
    '''
    This function creates a new dataframe that produces three columns and a count of
    least accessed lesson plans by cohort.
    '''
    
    least_df= df.groupby(['cohort_id', 'path']).agg('count')
    least_df.sort_values(by= 'user_id', ascending= False)
    least_df= least_df.reset_index()
    least_df= least_df[least_df.path != '/']
    new_least_df= least_df[['cohort_id', 'path', 'ip']]
    new_least_df.sort_values(by= ['cohort_id', 'ip'], ascending= [False, False])
    newer_df= new_least_df.sort_values(by= 'ip', ascending= False).groupby('cohort_id').nth(-1)
    newer_df.rename(columns= {'ip': 'count'}, inplace= True)
    return newer_df

def q_seven_two(df):
    
    """
    This function creates a new dataframe that produces three columns and a count of
    least accessed lesson plans by program. 
    """
    
    least_df= df.groupby(['program_id', 'path']).agg('count')
    least_df.sort_values(by= 'user_id', ascending= False)
    least_df= least_df.reset_index()
    least_df= least_df[least_df.path != '/']
    new_least_df= least_df[['program_id', 'path', 'ip']]
    new_least_df.sort_values(by= ['program_id', 'ip'], ascending= [False, False])
    newer_df= new_least_df.sort_values(by= 'ip', ascending= False).groupby('program_id').nth(-1)
    newer_df.rename(columns= {'ip': 'count'}, inplace= True)
    return newer_df


def q_six(df):
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
