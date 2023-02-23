import os
import pandas as pd



def df_to_csv(df):
    filename = "curriculum_logs.csv"
    try:
        df.to_csv(filename, index=False, mode='x')
    except FileExistsError:
        df.to_csv(filename,index=False)

def offline_lesson_kernel_restart():
    filename = "curriculum_logs.csv"
    exists = os.path.isfile(filename)
    if exists:
        df = pd.read_csv(filename)
        
        return df
    else:
        #Define query
        query = '''
                select * 
                from logs
                JOIN cohorts ON logs.cohort_id=cohorts.id;
                '''
        #Define url
        url = get_db_url('curriculum_logs')
        
        #Read data from SQL server
        df = pd.read_sql(query, url, index_col="date")
        
        #Cache
        df_to_csv(df)
        
        return df
