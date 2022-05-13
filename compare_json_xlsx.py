import pprint
from functools import reduce
from googlesearch import search
import googlesearch
import functools
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import numpy as np
import pandas as pd
import re
import json

filename = 'search_result_autofficina_trentino.json'
# open search_result_autofficina_trentino.json and close it
with open(filename, 'r') as f:
    page_dict = json.load(f)
    f.close()

[print(key, page_dict[key]) for key in page_dict]
# count entry in dict where code is not empty
print("\n", len([page_dict[key] for key in page_dict if page_dict[key]["code"] != []]))
# count 'no code found' entries in page_dict['code']
print("no code found", len([page_dict[key] for key in page_dict if page_dict[key]["code"] == ['no code found']]))


# read known_subjects.xlsx and create a dataframe
df = pd.read_excel("known_subjects.xlsx")
print("df", df.columns)
# print df with column "VAT"
print("df", df['VAT'], type(df['VAT'][1]))

count = 0
found_bool = False
for key in page_dict:
    found_bool = False
    # print(key, page_dict[key]['code'], page_dict[key].keys())
    for code in page_dict[key]["code"]:
        if not df.loc[df['VAT'] == code].empty:
            print(df.loc[df['VAT'] == code])
            found_bool = True
    if found_bool:
        count += 1
        # for i in range(len(df)):
        #     if df['VAT'][i] == code and code != "no code found":
        #         print("! code", code, "is in df", key, i)
            # else:
                # print("code", code, "is not in df", df['VAT'][i], type(code), type(df['VAT'][i]))

print("total entries web", len(page_dict))
print("no code found", len([page_dict[key] for key in page_dict if page_dict[key]["code"] == ['no code found']]))
print("total entries df", len(df))
print("entries found", count)
print("entries w/o match", len(page_dict) -
      len([page_dict[key] for key in page_dict if page_dict[key]["code"] == ['no code found']]) - count)
