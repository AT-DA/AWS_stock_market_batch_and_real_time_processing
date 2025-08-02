import boto3

kinesis_client = boto3.client('kinesis')


#this function generates simulated stock prices for a set of companies using a random range (+/-10%) of the current price
#it returns a list of dictionaries, each item in the list is a dictionary that contains the symbol name and the price
def create_stock_market_data():

    from random import randrange
    from datetime import datetime, timedelta
    
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
                                
    return stock_data

#This function receives a list of dictionaries as an input parameter, then tranfsorms and 
#encodes it (change from string to bytes), then pushes it into a stream

def write_records_to_stream(records, event, context = None):
    
    import json
    
    #generate stock market data list
    records = create_stock_market_data()
    
    encoded_records = [] #this empty list will contain the encoded list
    
    #for each record, convert the data from a dictionary to json to be able to encode it.
    #encoding is converting data from one format into another usually into bytes, which is the raw format
    #computers and services like Kinesis expect. It is stated in the documentation of the function put_records that
    #data should be in bytes
    #Creating the partition key is necessary since all data records with the same partition key
    #map to the same shard(partition) within the stream(topic).
    for record in records:
        encoded_records.append(
                                {'Data': json.dumps(record).encode('utf-8'),
                                'PartitionKey': record['symbol']
                                    }
                                    )
    
     # Send the records batch to Kinesis
    response = kinesis_client.put_records(Records = encoded_records, StreamName = 'stock_prices')
    
    return response
