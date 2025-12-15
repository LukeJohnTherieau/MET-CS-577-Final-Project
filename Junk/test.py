from MongoDB import MongoDB
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Process
import multiprocessing


def load_amazon_data(mongo_db:MongoDB, amazon_reviews_db:MongoDB, category:str, url:str):
    if category not in mongo_db:
        print(category, mongo_db)
        for df in pd.read_json(path_or_buf = url, compression = "gzip",lines = True,chunksize = 10000):
            mongo_db.append(
                **{
                    "collection_name" : category,
                    "df" : df
                } | (
                    {
                        "timeseries" : {
                            "timeField" : "timestamp"
                        }
                    } if mongo_db == amazon_reviews_db else {}
                )
            )
    
if __name__ == '__main__':
    multiprocessing.set_start_method('spawn', force=True)
    amazon_reviews_db = MongoDB("Amazon_Reviews")
    amazon_items_db = MongoDB("Amazon_Items")
    response = requests.get("https://amazon-reviews-2023.github.io/")
    soup  = BeautifulSoup(response.text, "html.parser")
    file_links = soup.find_all(
        name = "a",
        attrs = {
            "href" : re.compile(r'.*https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/.*')
        }
    )
    for file_link in file_links:
        categories = file_link.attrs["href"].split("/")[-1].split(".jsonl.gz")[0].partition("meta_")
        mongo_db = amazon_reviews_db if categories[-1] == "" else amazon_items_db
        category = categories[0] if categories[-1] == "" else categories[-1]
        Process(
            target = load_amazon_data, 
            args = (
                mongo_db, 
                amazon_reviews_db, 
                category, 
                file_link.attrs["href"],
            )
        ).start()