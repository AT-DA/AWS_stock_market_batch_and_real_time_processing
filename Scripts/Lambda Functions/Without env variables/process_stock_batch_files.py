def process_batch_files_lambda(event, context):
    
    import pandas as pd
    from datetime import datetime, timedelta
    from random import randrange
    import awswrangler as wr

    
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    path = source_bucket + key
    
    df_new_file = wr.s3.read_csv(path)
    df_new_file['date'] = df_new_file['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date()) 
    
    df_athena = wr.athena.read_sql_query('SELECT * FROM price_by_date', database = 'stock_market')
    
    # Perform the left join and get records in df_new_file and not in df_athena
    merged_df = pd.merge(df_new_file, df_athena, how='left', left_on=['company', 'date'], right_on=['company', 'close_date'] )
    df_new_records = merged_df[merged_df['close_price_y'].isnull()][['company', 'date', 'close_price_x']]
    df_new_records.columns = ['company', 'close_date', 'close_price']
    
    df_new_records['p_year'] = pd.to_datetime(df_new_records['close_date']).dt.year
    
    dest_path = 's3://stock-market-raw-data-us-east-1/price_by_date/'

    wr.s3.to_parquet(df=df_new_records, path=dest_path, index=False, dataset=True, partition_cols=['p_year'])


 