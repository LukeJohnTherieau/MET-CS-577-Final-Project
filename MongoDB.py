from typing import Generator
import pandas as pd
import pymongo


class MongoDB:
    def __init__(self, database_name:str, url:str = "mongodb://localhost:27017/"):
        self.__database_name = database_name
        self.__url = url
        
        
    def append(self, df:pd.DataFrame, collection_name:str, timeseries:dict = None) -> bool:
        if not timeseries:
            timeseries = {}
            
        else:
            timeseries = {
                "timeseries" : timeseries
            }
            
        with pymongo.MongoClient(self.__url) as client:
            db = client[self.__database_name]
            if collection_name not in db.list_collection_names():
                db.create_collection(**{"name" : collection_name} | timeseries)
                
            db[collection_name].insert_many(
                df.to_dict(
                    orient = "records"
                )
            )
            
            
    def query(self, collection_name:str, query:dict = {}):
        rows = []
        with pymongo.MongoClient(self.__url) as client:
            db = client[self.__database_name]
            cursor  = db[collection_name].find(
                filter = query
            )
            previous_retrieved = 0
            for row in cursor:
                if len(row) > 0 and cursor.retrieved != previous_retrieved:
                    previous_retrieved = cursor.retrieved
                    yield pd.DataFrame(rows)
                    rows.clear()
                        
                rows.append(row)
                
            yield pd.DataFrame(rows)
    
    
    def __contains__(self, collection_name):
        with pymongo.MongoClient(self.__url) as client:
            db = client[self.__database_name]
            return collection_name in db.list_collection_names()
        
        
    def __repr__(self):
        return self.__database_name