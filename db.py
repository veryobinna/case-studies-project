import pymongo
from my_secrets import db_username, db_password 

database_name = "case-study-db"
mongo_uri = f"mongodb://{db_username}:{db_password}@localhost:27017/{database_name}"
client = pymongo.MongoClient(mongo_uri)
database = client[database_name]