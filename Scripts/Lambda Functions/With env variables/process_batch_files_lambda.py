def process_batch_files_lambda(event, context):

    """
    This function gets triggered by an S3 PUT event. It performs the following:
    
    1. Reads a newly uploaded CSV file containing stock data from an S3 bucket.
    2. Cleans the data (ensures proper types, removes bad records).
    3. Compares the incoming data with existing records in an Athena table.
    4. Appends only new records to a partitioned Parquet dataset on S3.
    
    Environment Variable:
    - dest_bucket_path: S3 path to save cleaned parquet files (must end with '/')
    
    Parameters:
    - event: dict, AWS Lambda event (S3 PUT)
    - context: object, AWS Lambda context
    
    Libraries Used:
    
    logging       → For handling logs in Lambda
    os            → To fetch environment variables
    pandas        → For data manipulation and cleaning
    datetime      → To handle timestamps and date logic
    awswrangler   → For interaction with AWS services (S3, Athena)  
    
    """


    
    
    import logging
    import os
    import pandas as pd
    from datetime import datetime, timedelta
    import awswrangler as wr

          
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
            
            
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    path = f"s3://{source_bucket}/{key}"
    
    try:
        dest_bucket_path = os.environ['dest_bucket_path']
    except Exception as e:
        logger.error(f'Error when reading the environment variable of the destination path {type(e).__name__} - {e}')
        raise #terminate the program
       
    
    logger.debug(f'reading {path}')
    
    try:
        df_new_file = wr.s3.read_csv(path)
        df_new_file_records_count = df_new_file.shape[0]
        
    except Exception as e:
        logger.error(f'Error when reading the csv file from the path {path} {type(e).__name__} - {e}')
        raise #terminate the program
    
    if df_new_file.shape[0] == 0:
        logger.warning("Ingested file is empty. Exiting job early.")
        
        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(),0)
        
        return {
        "status": "success with warning",
        "records_appended": 0,
        "duration_seconds": duration
                }

    
    logger.info(f'reading from {path} and dataframe creation successful. {df_new_file_records_count} records ingested')
    
    logger.debug('reading data using athena')
    
    try:
        df_athena = wr.athena.read_sql_query('SELECT * FROM price_by_date', database = 'stock_market')
        
    except Exception as e:
        logger.error(f'Error when reading data using athena {type(e).__name__} - {e}')
        raise #terminate the program

 
    logger.debug('reading using athena successful')
    
    
    try:
        logger.debug(f'cleaning {path} if needed')
        
        logger.debug('cleaning the close_price column')
        
        #convert to integer and convert records that cannot be converted to NaN
        df_new_file['close_price'] = pd.to_numeric(df_new_file['close_price'], errors='coerce') 
        
        bad_records_count = df_new_file['close_price'].isna().sum()
        
        #remove null close_price records
        df_new_file = df_new_file.dropna(subset=['close_price'])  
        
        #convert to int64 because it is equivalent to BIGINT in athena (same data type of the target table)
        df_new_file['close_price'] = df_new_file['close_price'].astype('int64')  

        logger.debug('done cleaning the close_price column')
        
        
        logger.debug('cleaning the date column')
        
        
        #convert string date to date object and replace dates that cannot be converted to NAN (errors='coerce')
        df_new_file['date'] = pd.to_datetime(df_new_file['date'], errors='coerce').dt.date 
        
        bad_records_count = bad_records_count + df_new_file['date'].isna().sum()
        
        #drop records with NULL dates if found
        df_new_file = df_new_file.dropna(subset=['date'])
        
        
        logger.debug('done cleaning the date column')
        
        
        logger.debug('cleaning the company column')
        
        df_new_file['company'] = df_new_file['company'].astype('object')
        
        logger.debug('done cleaning the company column')
        
        logger.info(f"Number of records with bad data: {bad_records_count}")
 
        logger.debug('checking if new records exist')     
        
        # Perform the left join and get records in df_new_file and not in df_athena
        merged_df = pd.merge(df_new_file, df_athena, how='left', left_on=['company', 'date'], right_on=['company', 'close_date'])
        df_new_records = merged_df[merged_df['close_price_y'].isnull()][['company', 'date', 'close_price_x']]
        df_new_records_count = df_new_records.shape[0]
    
    except Exception as e:
        logger.error(f'error during cleaning or when checking if new records exist {type(e).__name__} - {e}')
        raise #terminate the program
    
    
    if df_new_records_count > 0:
         
        logger.info(f'{df_new_records_count} records will be appended')
        
        df_new_records.columns = ['company', 'close_date', 'close_price']

        try:
            #create the partition column
            df_new_records['p_year'] = pd.to_datetime(df_new_records['close_date']).dt.year
            wr.s3.to_parquet(df=df_new_records, path=dest_bucket_path, index=False, dataset=True, partition_cols=['p_year'])
            
        except Exception as e:
            logger.error(f'Error creating the partition column or when writing the file to {path} {type(e).__name__} - {e}')
            raise #terminate the program
            
        
        records_difference = df_new_file_records_count - df_new_records_count
        
        if records_difference == 0:
            end_time = datetime.now()
            duration = round((end_time - start_time).total_seconds(),0)
            
            logger.info(f'all records ({df_new_records_count}) got appended successfully to the table stock_market.price_by_date.\
                        Total time taken: {duration} seconds. ETL job will exit.')
                        
            return {
            "status": "success",
            "records_appended": df_new_records_count,
            "duration_seconds": duration
                    }
        
        else:
            logger.warning(f'destination records are less than the source records by {records_difference}')
            
            end_time = datetime.now()
            duration = round((end_time - start_time).total_seconds(),0)
            
            logger.info(f'{df_new_records_count}/{df_new_file_records_count} records got appended successfully \
            to the table stock_market.price_by_date. Total time taken: {duration} seconds. ETL job will exit.')
                    
            return {
            "status": "success with warning",
            "records_appended": df_new_records_count,
            "duration_seconds": duration
                    }
        
            
    else:
        end_time = datetime.now()
        duration = round((end_time - start_time).total_seconds(),0)
        
        logger.warning(f'No new records found after comparing the ingested data with the table stock_market.price_by_date \
        Total time taken: {duration} seconds. ETL job will exit.')
        
        return {
        "status": "success with warning",
        "records_appended": 0,
        "duration_seconds": duration
                }
    
    
    
   
    


 