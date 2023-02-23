#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd

def q_one(df):

    # cleaning for n/a 'path' values
    most_accessed_path_by_program = df[df['path'] != '/']
    most_accessed_path_by_program = most_accessed_path_by_program[most_accessed_path_by_program['path'] != 'index.html']
    most_accessed_path_by_program = most_accessed_path_by_program[most_accessed_path_by_program['path'] != 'toc']
    most_accessed_path_by_program = most_accessed_path_by_program[most_accessed_path_by_program['path'] != 'search/search_index.json']
    most_accessed_path_by_program = most_accessed_path_by_program[most_accessed_path_by_program['path'].str.contains('.jpg') == False]
    
    # groupby() 'program_id' & 'path' and counting
    # goal is to total accesses to a 'path' and dividing by 'program_id'
    most_accessed_path_by_program = most_accessed_path_by_program.groupby(['program_id', 'path']).agg('count')
    
    # saving groupby() result as dataframe to gain acces to 'path' column again
    most_accessed_path_by_program = most_accessed_path_by_program.reset_index()
    
    # subsetting dataframe again with only 3 colums
    most_accessed_path_by_program = most_accessed_path_by_program[['program_id', 'path', 'ip']]
    
    # groupby() 'program_id' which will count the path access from an 'ip'
    # the nth method will extract only the top result (being the largest count for each group)
    most_accessed_path_by_program = most_accessed_path_by_program.sort_values(by= 'ip', ascending= False).groupby('program_id').nth(0)
    
    # fixed dataframe column name to prepare for printing output
    most_accessed_path_by_program.rename(columns= {'ip': 'count'}, inplace= True)

    print(most_accessed_path_by_program)

def q_two(df):
    '''
    This function creates a new df that produces three columns and a count of 
    most accessed lesson plans by cohort.
    '''

    weird_df= df.groupby(['cohort_id', 'path']).agg('count')
    weird_df.sort_values(by= 'user_id', ascending= False)
    weird_df= weird_df.reset_index()
    weird_df= weird_df[weird_df.path != '/']
    weird_df= weird_df[weird_df.path != 'toc']
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
    least_df= least_df[least_df.path != 'toc']
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
    least_df= least_df[least_df.path != 'toc']
    new_least_df= least_df[['program_id', 'path', 'ip']]
    new_least_df.sort_values(by= ['program_id', 'ip'], ascending= [False, False])
    newer_df= new_least_df.sort_values(by= 'ip', ascending= False).groupby('program_id').nth(-1)
    newer_df.rename(columns= {'ip': 'count'}, inplace= True)
    return newer_df


def q_six(df):
    # subset dataframe of only inactive students
    # (accces date is after their class 'end_date')
    inactive_students = df.loc[df["is_active"] == 0]

    # extra cleaning to remove path names that aren't "Topics"
    most_accessed_path_by_program = inactive_students[active_students['path'] != '/']
    most_accessed_path_by_program = most_accessed_path_by_program[most_accessed_path_by_program['path'] != 'toc']
    most_accessed_path_by_program = most_accessed_path_by_program[most_accessed_path_by_program['path'] != 'search/search_index.json']
    most_accessed_path_by_program = most_accessed_path_by_program[most_accessed_path_by_program['path'].str.contains('.jpg') == False]

    # groupby() 'program_id' & 'path' and counting
    # goal is to total accesses to a 'path' and dividing by 'program_id'
    most_accessed_path_by_program = most_accessed_path_by_program.groupby(['program_id', 'path']).agg ('count')

    # saving groupby() result as dataframe to gain acces to 'path' column again
    most_accessed_path_by_program = most_accessed_path_by_program.reset_index()

    # subsetting dataframe again with only 3 colums
    most_accessed_path_by_program = most_accessed_path_by_program[['program_id', 'path', 'ip']]
    
    # groupby() 'program_id' which will count the path access from an 'ip'
    # the nth method will extract only the top result (being the largest count for each group)
    most_accessed_path_by_program = most_accessed_path_by_program.sort_values(by= 'ip', ascending= False).groupby('program_id').nth(0)
   
    # fixed dataframe column name to prepare for printing output
    most_accessed_path_by_program.rename(columns= {'ip': 'count'}, inplace= True)
   
    return most_accessed_path_by_program

def fourth_cohort(df):
    four= df[df.cohort_id== 4]
    return four

