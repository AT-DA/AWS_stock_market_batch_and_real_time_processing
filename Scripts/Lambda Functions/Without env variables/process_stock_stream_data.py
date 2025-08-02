def process_stock_stream_data(event, context):
    
    import json
    import boto3
    import awswrangler as wr
    import pandas as pd
    from datetime import datetime
    
    #get the source path of the json file that will be read
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    path = source_bucket + key
    
    #read the json file as a data frame
    df = wr.s3.read_json(path = path, lines=True) #lines=True is used to treat each line as a separate object
    
    #Apply Transformations
    df['price'] = df['price'].astype(int)
    df['produced_at'] = pd.to_datetime(df['produced_at'])
    df['etl_loading_ts'] = datetime.now()
    df['p_year'] = df['produced_at'].dt.year  #create the partition
    
    #write the file as parquet to the destination bucket
    wr.s3.to_parquet(df = df, path ='s3://stock-market-raw-data-us-east-1/price_by_date_stream/',
                        dataset=True, #This is done to enable other arguments of the function to_parquet such as partition_cols
                        mode='append', #add new records without any overwrite or updates
                        partition_cols=['p_year'])

    wr.s3.to_parquet(
                    df = df,
                     path ='s3://stock-market-raw-data-us-east-1/latest_stream_prices/latest_data.parquet'
                     )                   
                                                    