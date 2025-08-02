def generate_stock_batch_data(event = None, context = None):
    """
    
    Function Overview:
    ------------------
    This function generates random daily closing stock prices for a predefined list of companies
    between Jan 1, 2022 and Dec 31, 2024.

    The generated data is saved as a CSV file in a staging destination S3 path, specified using the 
    `dest_bucket_path` environment variable.
    
    The PK of the dataset is the day, and the closing_date

    Libraries used:
    - pandas: For dataframe creation
    - awswrangler: For writing to S3
    - datetime & random: For date and value simulation
    - logging: For debugging and error tracing

    Parameters:
    -----------
    event : dict, optional
        AWS Lambda event input (default is None)
    context : object, optional
        AWS Lambda context object (default is None)

    Environment Variables:
    ----------------------
    dest_bucket_path : str
        Full S3 URI (with trailing slash) where the generated CSV file will be saved.
        Example: "s3://my-bucket/stocks-data/staging/"

    Returns:
    --------
    None
    Outputs a CSV file to the staging S3 bucket.
    """

    
    import logging

    logger = logging.getLogger()

    """A logger handler in Python's logging module is a component that determines
    where the log messages go — such as a file, console, email,
    or an external service like AWS CloudWatch.
    AWS Lambda already has a preconfigured handler. 
    What is not preconfigured though is the log-level.
    So the if condition below checks if the logger object has an object, 
    if yes (in case of AWS lambda) then just set the logging level
    """
    #create and configure logging
    if logger.hasHandlers():
        logger.setLevel(logging.INFO)
    else:   
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
 

    try:
    
        import os
        import awswrangler as wr
        import pandas as pd
        from datetime import datetime, timedelta
        from random import randrange
            
    
    except Exception as e:
        logger.error(f'Error when importing one of the libraries: {type(e).__name__} - {e}')
        raise #terminate the program
     

    #get the current stock prices of some companies on 25/01/2025
    companies_price = {
                        'NVDA': {'current_price':143},
                        'AAPL': {'current_price':223},
                        'MSFT': {'current_price':444},
                        'AMZN': {'current_price':234},
                        'GOOGL': {'current_price':200},
                        'META': {'current_price':647},
                        'TSLA': {'current_price':407},
                        'WMT': {'current_price':95},
                        'JPM': {'current_price':265},
                        'V': {'current_price':330},
                        'ORCL': {'current_price':184},
                        'MA': {'current_price':490},
                        'XOM': {'current_price':109},
                        'NFLX': {'current_price':978},
                        'PG': {'current_price':164},
                        'SAP': {'current_price':276}
                        }
    
    #create a min and max price for each current price by adding and removing 10% from the current price
    for k in companies_price:
        companies_price[k]['min_price'] = int(companies_price[k]['current_price'] * 0.9)
        companies_price[k]['max_price'] = int(companies_price[k]['current_price'] * 1.1)
        
    #create a random date between 1/1/2022 and 31/12/2024

    date_1 = datetime.strptime('01-01-2022', '%d-%m-%Y')
    date_2 = datetime.strptime('31-12-2024', '%d-%m-%Y')

    days_diff_integer = (date_2 - date_1).days
    random_days = randrange(days_diff_integer)
    random_date = date_1 + timedelta(days = random_days)
    random_date = random_date.strftime('%Y-%m-%d') 
    
    #generate a random closing pricing between the min and the max prices, and add the random_date generated in the previous step
    for k in companies_price:
        min_price = companies_price[k]['min_price']
        max_price = companies_price[k]['max_price']
        companies_price[k]['date'] = random_date
        companies_price[k]['close_price'] = randrange(min_price, max_price)
        
    #delete the keys min_price, max_price and current_price since they're not needed anymore
        del companies_price[k]['min_price'] 
        del companies_price[k]['max_price']
        del companies_price[k]['current_price']    

    
    logger.debug('creating the dataframe')
    
    try:
        #create the dataframe
        df = pd.DataFrame.from_dict(companies_price, orient='index')
        df.reset_index(inplace = True)
        df.rename(columns={'index': 'company'}, inplace = True)
        df_count = str(df.shape[0])
     
    except Exception as e:
       
        logger.error(f'Error when creating the dataframe from the dictionary {type(e).__name__} {e}')
        raise
    
    logger.debug('dataframe created')
    
    logger.debug('exporting to s3')
    
    try:
        #export the csv to s3
        today = datetime.today()
        file_name = datetime.strftime(today, '%Y%m%d%H%M%S')
        dest_bucket_path = os.environ['dest_bucket_path'] #get the value of the environment value
        path = f'{dest_bucket_path}{file_name}.csv'
        wr.s3.to_csv( df = df, path = path, index = False)
     
    except Exception as e:
        logger.error(f'Error when exporting the file {type(e).__name__} {e}')
        raise #terminate the program
    
    logger.info(f'file exported to {path} with {df_count} records and stock close_date {random_date}')
    
