from pymongo import MongoClient
import mysql.connector

mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'password'
mysql_db = 'classicmodels'

mongo_uri = 'mongodb://localhost:27017/'
mongo_db_name = 'customers'
mongo_collection_name = 'customers'

mysql_conn = mysql.connector.connect(
    host=mysql_host, 
    user=mysql_user, 
    password=mysql_password, 
    database=mysql_db
)

print("successfull connection")

mysql_cursor = mysql_conn.cursor()

mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db_name]
mongo_collection = mongo_db[mongo_collection_name]

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