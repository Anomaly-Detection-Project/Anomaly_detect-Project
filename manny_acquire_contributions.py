def df_to_csv(df):
    try:
        df.to_csv(filename, index=False, mode='x')
    except FileExistsError:
        df.to_csv(filename,index=False)

def offline_lesson_kernel_restart():
    exists = os.path.isfile(filename)
    if exists:
        df = pd.read_csv(filename)
        
        return df
    else:
        #Define query
        query = '''
                SELECT *
                FROM logs
                '''
        #Define url
        url = get_db_url('curriculum_logs')
        
        #Read data from SQL server
        df = pd.read_sql(query, url)
        
        #Cache
        df_to_csv(df)
        
        return df

