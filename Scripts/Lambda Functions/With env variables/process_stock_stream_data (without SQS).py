def process_stock_stream_data(event, context):
"""
this function is similar to the code in process_stock_stream_data.py but gets triggered 
by S3 put events instead of a stream of events in SQS.
"""
    
    import os
    import json
    import boto3
    import awswrangler as wr
    import pandas as pd
    from datetime import datetime
    import logging
    
    # Start timestamp
    start_time = datetime.now()

    #create a logger object
    logger = logging.getLogger()
    
    
    """
    botocore.credentials is a library-specific logger inside the AWS SDK — logs internal activity, like how it loads credentials.
    it is different than the logger being created above logger = logging.getLogger()
    Why are there multiple loggers?
    The logging module supports a hierarchical naming system, allowing you to control logging granularly:
    logging.getLogger() → the root logger
    logging.getLogger('my_app') → custom logger for your code
    logging.getLogger('botocore.credentials') → logger for AWS SDK internals
    This way, you can filter logs from third-party libraries without muting your own logs.
    """
    #Only show WARNING and above from botocore.credentials, ignore INFO like the ‘Found credentials’ message.
    logging.getLogger('botocore.credentials').setLevel(logging.WARNING) 

    """A logger handler in Python's logging module is a component that determines
    where the log messages go — such as a file, console, email,
    or an external service like AWS CloudWatch.
    AWS Lambda already has a preconfigured handler. 
    What is not preconfigured though is the log-level.
    So the if condition below checks if the logger object has an object, 
    if yes (in case of AWS lambda) then just set the logging level
    """
    #configure the logging object
    if logger.hasHandlers():
        logger.setLevel(logging.INFO)
    else:   
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )   
        
            
    
    #get the source path of the json file that will be read
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    path = f's3://{source_bucket}/{key}'
    
    try:
        #read the json file as a data frame
        df = wr.s3.read_json(path = path, lines=True) #lines=True is used to treat each line as the delimiter
    except Exception as e:
        logger.error(f'Error when reading the input stream data from {path}  {type(e).__name__} - {e}')
        raise #terminate the program
    
    try:
        dest_bucket_path_overwrite = os.environ['dest_bucket_path_overwrite']
    except Exception as e:
        logger.error(f'Error when reading the environment variable of the overwrite \
        destination path: {dest_bucket_path_overwrite}  {type(e).__name__} - {e}')
        raise #terminate the program
    
    try:
        dest_bucket_path = os.environ['dest_bucket_path']
    except Exception as e:
        logger.error(f'Error when reading the environment variable of the \
        destination path: {dest_bucket_path}  {type(e).__name__} - {e}')
        raise #terminate the program
        
        
    try:
        df_athena_test = wr.athena.read_sql_query(
            sql="SELECT 1 test_column from stream_prices_history \
            union all \
            SELECT 1 test_column from latest_prices",  
            database="stock_market"
        )
    except Exception as e:
        logger.error(f'athena connection failed or one of the destination tables do not exist {type(e).__name__} - {e}')
        raise  
      
         
    if df.shape[0] == 0:
        logger.warning("Ingested file is empty. Exiting job early.")
        
        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(),0)
        
        return {
        "status": "success with warning",
        "records_appended": 0,
        "duration_seconds": duration
                }
    
    
   
    try:
        #Apply Transformations

        #convert to integer and convert records that cannot be converted to NaN
        df['price'] = pd.to_numeric(df['price'], errors='coerce') 
        
        bad_records_count = df['price'].isna().sum()
        
        #remove null price records
        df = df.dropna(subset=['price'])  
        
        #convert to int64 because it is equivalent to BIGINT in athena (same data type of the target table)
        df['price'] = df['price'].astype('int64')  

        #convert string date time to date time object and replace date time that cannot be converted to NAN (errors='coerce')
        df['produced_at'] = pd.to_datetime(df['produced_at'], errors='coerce')
        
        bad_records_count = bad_records_count + df['produced_at'].isna().sum()
        
        #drop records with NULL dates if found
        df = df.dropna(subset=['produced_at'])
        
        df['symbol'] = df['symbol'].astype('object')
        
        
        logger.info(f"Number of records with bad data: {bad_records_count}")
        
        etl_loading_ts = pd.to_datetime(datetime.now())
        df['etl_loading_ts'] = etl_loading_ts
        df['p_year'] = df['produced_at'].dt.year  #create the partition
        
    except Exception as e:
        logger.error(f'Error when applying transformations {type(e).__name__} - {e}')
        raise #terminate the program
    
    #write the file as parquet to the destination bucket
    
    try:
        wr.s3.to_parquet(df = df, path = dest_bucket_path,
                            dataset=True, #This is done to enable other arguments of the function to_parquet such as partition_cols
                            mode='append', #add new records without any overwrite or updates
                            partition_cols=['p_year'])
    except Exception as e:
            logger.error(f'Error when writing the file to {dest_bucket_path} {type(e).__name__} - {e}')
            raise #terminate the program
        
        
    latest_prices_query =  'select symbol, price, produced_at \
                            from \
                            ( \
                                select symbol, price, produced_at , row_number() over(partition by symbol order by produced_at desc) ranking\
                                from stream_prices_history\
                                where p_year = year(produced_at)\
                            ) a \
                            where ranking = 1;\
                            '
    
    try: 
        df_athena = wr.athena.read_sql_query(sql = latest_prices_query, database = 'stock_market')
        df_athena['etl_loading_ts'] = etl_loading_ts
    except Exception as e:
        logger.error(f'Error when reading data using athena {type(e).__name__} - {e}')
        raise 
    
    try:
        wr.s3.to_parquet(
                        df = df_athena,
                         path = dest_bucket_path_overwrite
                         )                   
    except Exception as e:
            logger.error(f'Error when writing the file to {dest_bucket_path_overwrite} {type(e).__name__} - {e}')
            raise #terminate the program       

    if bad_records_count == 0:
        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(),0)
        
        logger.info(f'all records ({df.shape[0]}) got appended successfully to the tables stock_market.price_by_date_streams.\
                    and overritten in stock_market.price_by_date_latest. Total time taken: {duration} seconds. ETL job will exit.')
                    
        return {
        "status": "success",
        "records_appended": df.shape[0],
        "duration_seconds": duration
                }
        
    else:
        logger.warning(f'destination records are less than the source records by {bad_records_count}')
        
        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(),0)
        
        logger.info(f'{df.shape[0]}/{df.shape[0] + bad_records_count} records  got appended successfully \
        to the tables stock_market.price_by_date_streams and overritten in stock_market.price_by_date_latest. \
        Total time taken: {duration} seconds. ETL job will exit.')
                
        return {
        "status": "success with warning",
        "records_appended": df.shape[0],
        "duration_seconds": duration
                }