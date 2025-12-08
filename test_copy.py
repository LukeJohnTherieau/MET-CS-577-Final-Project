from MongoDB import MongoDB
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Process
import multiprocessing
import requests
from bs4 import BeautifulSoup
import re
from MongoDB import MongoDB
from MongoDB import MongoDB
import pandas as pd
from multiprocessing import Process


def load_amazon_data(mongo_db:MongoDB, amazon_reviews_db:MongoDB, category:str, url:str):
    if category in mongo_db:
        print(category, mongo_db)
        for df in pd.read_json(path_or_buf = url, compression = "gzip",lines = True,chunksize = 200_000):
            print("Loading chunk... {} rows".format(len(df)))
            # Process(
            #     target = mongo_db.append,
            #     args = (
            #         df,
            #         category,
            #         (
            #             {"timeField" : "timestamp"} 
            #             if mongo_db == amazon_reviews_db
            #             else {}
            #         ),
            #     )
            # ).start()
            start_date = df["timestamp"].min().strftime("%Y-%m-%d")
            end_date = df["timestamp"].max().strftime("%Y-%m-%d")
            Process(
                target = df.to_feather,
                args = (
                    f"data/{start_date}_{end_date}.ftr",
                )
            ).start()
        
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
        if categories[0] == "Video_Games":# or categories[-1] == "Video_Games":
            print(categories)
            mongo_db = amazon_reviews_db if categories[-1] == "" else amazon_items_db
            category = categories[0] if categories[-1] == "" else categories[-1]
            load_amazon_data(
                mongo_db, amazon_reviews_db, category, file_link.attrs["href"])