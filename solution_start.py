import argparse
import pandas as pd
import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import logging

def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())

	

def initilize_spark():
    try:
        spark=SparkSession.builder.appName('Practice').getOrCreate()
	# created spark session object 
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        logging.info('Spark Session Created')
	# logging 
    except:
        spark==None
        print("Unable to Create Spark Session")
    return spark


def load_transaction_data(transactions_location,spark,count_transactions=None):
    logging.warning('Number of transaction files to load:',count_transactions)
    transaction_files = os.listdir(transactions_location) #gets the list of folder names
    #print("Transaction Directory Names")
    #print(transaction_files)
    if count_transactions==None:
        count_transactions= len(transaction_files)
    # if want to load only limited number of transcation then pass the last argument in this function, else leave as none, to load all transactions
    count_transactions = len(transaction_files)
    if len(transaction_files)>0:
        transaction_data = spark.read.json(transactions_location+transaction_files[0]+'/transactions.json')
    c=1
    for transaction_file in transaction_files[1:]:
	# load transaction json files one by one and merge them into one pyspark dataframe
        transaction_data_part = spark.read.json(transactions_location+transaction_file+'/transactions.json') # load a datafrane
        transaction_data = transaction_data.union(transaction_data_part) #union merges the two dataframe
        c+=1
        if c==count_transactions:
            break
	
    #print("Transaction Data")
    #print(transaction_data.show())
    logging.warning('Transaction Data loaded Successfully')
    return transaction_data

def explode_basket(dataframe):
    df_exploded = dataframe.withColumn(('basket'), explode('basket'))
    df_exploded = df_exploded.select('basket.*', 'customer_id','date_of_purchase')
    logging.warning('Expanded DataFrame')
    return df_exploded

def pre_process_query(transaction,customers,products,spark): # to generate the output i.e, required columsn only by joining the three tables
    customers = spark.createDataFrame(customers)
    products = spark.createDataFrame(products)  # to convert from pandas to spark dataframe
    transaction.createOrReplaceTempView("transactionsView") # to register the dataframe to the pandas session, to execute sql queries
    products.createOrReplaceTempView("productsView")
    customers.createOrReplaceTempView("customersView")
    query1="SELECT t1.product_id,t2.product_category,t1.customer_id,count(*) as purchase_count  FROM transactionsView AS t1 INNER JOIN productsView AS t2 ON t1.product_id=t2.product_id GROUP BY t1.product_id,t2.product_category,t1.customer_id"
    # got product id, product category, customer id and the count for each type of product id
    product_category_counts = spark.sql(query1) # execut ethe query
    product_category_counts.createOrReplaceTempView("product_category_counts") #saved the resultant table
    #print(product_category_counts.show())
    query2="SELECT t2.customer_id,t2.loyalty_score,t1.product_id,t1.product_category,t1.purchase_count FROM product_category_counts AS t1 INNER JOIN customersView AS t2 ON t1.customer_id=t2.customer_id" #this query to join the third table to get the cusottomer's loyalty value
    final_result = spark.sql(query2) #execute query
    #print("Resultant Data:")
    #print(final_result.show())
    logging.warning('Pre-Processing Complete')
    return final_result

def toJSON(dataframe, location): # to svae the result table as json file
    logging.warning('Saving output file to: ',location)
    pandas_df = dataframe.toPandas() #converted to pandas dataframe from pyspark dataframe - to call to_json() function, which creates json file on disk
    if not os.path.exists(location):
        os.makedirs(location)
    pandas_df.to_json(location+"output.json") #saved as json file
    logging.warning('Output File Generated')

def main():
    params = get_params() # got locations of the csv and json files
    spark = initilize_spark() #initialized spark session

    customers_data = pd.read_csv(params['customers_location'])   #loaded csv using pandas
    #print("Customers Location")
    #print(customers_data)
    products_data = pd.read_csv(params['products_location'])
    #print("Products Location")
    #print(products_data)

    logging.info('Customers and Products Data Loaded')

    transaction_data = load_transaction_data(params['transactions_location'],spark)
    transaction_data_exploded = explode_basket(transaction_data) #since it was nested json, so needed to expand the array of data  

    #print("Transaction Data Exploded")
    #print(transaction_data_exploded.show())

    result = pre_process_query(transaction_data_exploded,customers_data,products_data,spark)
    toJSON(result,params['output_location'])

if __name__ == "__main__":  
    main()


# output json: customer_id, loyalty_score, product_id, product_category, purchase_count
