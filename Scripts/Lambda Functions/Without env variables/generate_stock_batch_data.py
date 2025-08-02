def generate_stock_batch_data(event = None, context = None):
    
    import os
    import awswrangler as wr
    import pandas as pd
    from datetime import datetime, timedelta
    from random import randrange
    
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
    
    #create the dataframe
    df = pd.DataFrame.from_dict(companies_price, orient='index')
    df.reset_index(inplace = True)
    df.rename(columns={'index': 'company'}, inplace = True)
    
    #export the csv to s3
    today = datetime.today()
    file_name = datetime.strftime(today, '%Y%m%d%H%M%S')
    path = f's3://stock-market-raw-data-us-east-1/stg_price_by_date/{file_name}.csv'
    wr.s3.to_csv( df = df, path = path, index = False)
    
