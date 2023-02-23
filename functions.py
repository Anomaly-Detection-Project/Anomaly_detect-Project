#!/usr/bin/env python
# coding: utf-8

# In[ ]:


def Q_two(df):
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
    return new_df

def Q_three(df):
    '''
    This function creates a new df that produces two columns with only active user
    log in attempts and a count of least active users.
    '''
    
    df1 = df.loc[df["is_active"] ==1 ]
    weird_df2= df.groupby(['user_id']).agg('count')
    weird_df2.sort_values(by= 'cohort_id', ascending= False)
    weird_df2= weird_df2.reset_index()
    weird_df2= weird_df2[weird_df2.ip < 50]
    count_df2= weird_df2[['user_id', 'ip']]
    count_df2.sort_values(by= ['user_id', 'ip'], ascending= [False, False])
    new_df2= count_df2.sort_values(by= 'ip', ascending= False).groupby('user_id').nth(0)
    return new_df2


def q_seven(df):
    '''
    This function creates a new df that produces three columns and a count of
    least accessed lesson plans by cohort.
    '''
    
    least_df= df.groupby(['cohort_id', 'path']).agg('count')
    least_df.sort_values(by= 'user_id', ascending= False)
    least_df= least_df.reset_index()
    least_df= least_df[least_df.path != '/']
    new_least_df= weird_df[['cohort_id', 'path', 'ip']]
    new_least_df.sort_values(by= ['cohort_id', 'ip'], ascending= [False, False])
    newer_df= new_least_df.sort_values(by= 'ip', ascending= False).groupby('cohort_id').nth(-1)
    return newer_df

