    """
    Function Overview:
    ------------------
    This function pushes a simulated batch of stock price records to an Amazon Kinesis Data Stream. 
    The data is first generated using the `create_stock_market_data()` function, then encoded, and 
    finally sent to the stream in batch using the `put_records` API.

    Each record includes:
    - stock symbol
    - randomly generated price (+/- 10% of a base value)
    - record generation timestamp

    Records are newline-separated and UTF-8 encoded to ensure compatibility with downstream consumers
    like Amazon Kinesis Firehose and S3.

    Libraries used:
    - boto3: For interfacing with the AWS Kinesis Data Stream
    - json: For serializing records to JSON
    - logging: For structured logging
    - datetime: For timestamping records

    Parameters:
    -----------
    records : list
        List of stock record dictionaries to be published.It's Generated using 
        the function via `create_stock_market_data().
    
    event : dict
        AWS Lambda event input ( AWS Lambda always passes it automatically when the function is invoked)

    context : object, optional
        AWS Lambda context object (default is None)

    Environment Variables:
    ----------------------
    stream_name : str
        Name of the Kinesis Data Stream to which records will be pushed.
        Example: "stock-stream-raw"

    Returns:
    --------
    dict
        {
            'FailedRecordCount': int,
            'SuccessRecordCount': int
        }

    Notes:
    ------
    - All records in the batch share the same `produced_at` timestamp.
    - A newline `\n` is added to each record to simplify parsing when data lands in flat-file storage.
    - The partition key is set to the stock symbol to evenly distribute across shards.
    - Logging captures batch success/failure count and runtime duration for monitoring.
    """
import boto3
import os
from datetime import datetime, timedelta
import logging



#create a kinesis client
kinesis_client = boto3.client('kinesis')


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

try: 
    stream_name = os.environ['stream_name']
except Exception as e:
        logger.error(f'Error when retrieving the input variable of the stream_name {type(e).__name__} - {e}')
        raise #terminate the program 




#this function generates simulated stock prices for a set of companies using a random range (+/-10%) of the current price
#it returns a list of dictionaries, each item in the list is a dictionary that contains the symbol name and the price

def create_stock_market_data():

    from random import randrange, sample
    
    
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
    
    
    stock_data = []
    
    for k in companies_price:
        
        #create a min and max price for each current price by adding and removing 10% from the current price
        companies_price[k]['min_price'] = int(companies_price[k]['current_price'] * 0.9)
        companies_price[k]['max_price'] = int(companies_price[k]['current_price'] * 1.1)
        
        #generate a new random current price between the min and the max price
        companies_price[k]['current_price'] = randrange(companies_price[k]['min_price'], companies_price[k]['max_price'])
        
        #delete the keys min_price, max_price since they're not needed anymore
        del companies_price[k]['min_price'] 
        del companies_price[k]['max_price']
        
        current_timestamp = datetime.now()
        
        stock_data.append({'symbol': k, 'price': companies_price[k]['current_price'], 
                           'produced_at': current_timestamp.strftime('%Y-%m-%d %H:%M:%S')})
         
    stock_data = sample(stock_data, randrange(5,16))
        
    return stock_data

#This function receives a list of dictionaries as an input parameter, then tranfsorms and 
#encodes it (change from string to bytes), then pushes it into a stream

def write_records_to_stream(records, event, context = None):
    
    import json
    
    
    try:
        #generate stock market data list
        records = create_stock_market_data()
    
    except Exception as e:
        logger.error(f'Error when generating the input data list: {type(e).__name__} - {e}')
        raise #terminate the program
            
    encoded_records = [] #this empty list will contain the encoded list
    
    """
    for each record, convert the data from a dictionary to json to be able to encode it.
    before converting to json, a new line is added manually at the end of each list element because
    when those records are written to S3, they may be concatenated into one output file  without 
    any delimiter between them. So this new line will act as a delimiter
    to parse those records later during the processing phase. Therefore, although create_stock_market_data() generates
    many records into the stream, if more than one record appears in the same file after loading by firehose to s3,
    those records will be concatenated unless a new line separator is added manually to make parsing easier.
    encoding is converting data from one format into another usually into bytes, which is the raw format
    computers and services like Kinesis expect. It is stated in the documentation of the function put_records that
    data should be in bytes
    Creating the partition key is necessary since all data records with the same partition key
    map to the same shard(partition) within the stream(topic).
    """
    for record in records:
        encoded_records.append(
                                {'Data': (json.dumps(record) + '\n').encode('utf-8'), #convert each list element to json then encode
                                'PartitionKey': record['symbol'] #create the partition key for each record
                                    }
                                    )
    
    try:
        # Send the records batch to Kinesis
        response = kinesis_client.put_records(Records = encoded_records, StreamName = stream_name)
    except Exception as e:
        logger.error(f'Error when pushing the data to the stream {stream_name} {type(e).__name__} - {e}')
        raise #terminate the program
    
    end_time = datetime.now()
    duration = round((end_time - start_time).total_seconds(),0)
        
    logger.info(f'number of records that should enter the stream {stream_name} at {records[0]["produced_at"]} \
    are:  {len(records)}. Total time taken: {duration} seconds. ETL job will exit.')
    
    return {'FailedRecordCount': response['FailedRecordCount'],
            'SuccessRecordCount': len(records)}
