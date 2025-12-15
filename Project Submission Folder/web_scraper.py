import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from multiprocessing import Process
import multiprocessing
import os

        
if __name__ == '__main__':
    multiprocessing.set_start_method('spawn', force=True)
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
        if categories[0] == "Video_Games":
            for i, df in enumerate(pd.read_json(path_or_buf = file_link.attrs["href"], compression = "gzip", lines = True, chunksize = 200_000)):
                print(f"Loading chunk {i} of {len(df)} rows")
                if os.path.exists("data") is False:
                    os.mkdir("data")
                
                Process(
                    target = df.to_feather,
                    args = (
                        f"data/{i}.ftr",
                    )
                ).start()