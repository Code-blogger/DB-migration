from pymongo import MongoClient
import mysql.connector

mysql_host = 'localhost'     # mysql Host
mysql_user = 'root'          # mysql User
mysql_password = 'password'  # mysql Password
mysql_db = 'classicmodels'   # mysql Database

mongo_uri = 'mongodb://localhost:27017/'  # mongo URI
mongo_db_name = 'customers'               # mongo Database 
mongo_collection_name = 'customers'       # Mongo Collection

mysql_conn = mysql.connector.connect(  # Mysql Connection
    host=mysql_host, 
    user=mysql_user, 
    password=mysql_password, 
    database=mysql_db
)

print("successfull connection")

mysql_cursor = mysql_conn.cursor()   # Mysql cursor

mongo_client = MongoClient(mongo_uri)  # Mongo Client URI
mongo_db = mongo_client[mongo_db_name] # Mongo Database
mongo_collection = mongo_db[mongo_collection_name] # Mongo Collection

mysql_cursor.execute("SELECT * FROM classicmodels.customers") 
mysql_data = mysql_cursor.fetchall()

for row in mysql_data:
    mongo_document = {
    'Customer Number' : row[0],
    'Customer Name' : row[1],
    'Contact Name' : row[2] + row[3],
    'Phone' : row[4],
    'Address Line 1' : row[5],
    'Address Line 2' : row[6],
    'City' : row[7],
    'State' : row[8],
    'Postal Code' : row [9],
    'Country' : row[10],
    'Sales Rep Employee Number': row[11],
    'Credit Limit': int(row[12])
    }
    
    mongo_collection.insert_one(mongo_document) 

print("Successfull Upload to Mongdb.")


mysql_cursor.close()
mysql_conn.close()
mongo_client.close()