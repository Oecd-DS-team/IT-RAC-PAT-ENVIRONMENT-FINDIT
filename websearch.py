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
from tqdm import tqdm
import os
import joblib
from joblib import Parallel, delayed
import contextlib
# from WebDriver import WebDriver
# googleDriver = WebDriver()
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# options = webdriver.ChromeOptions()
# options.add_argument('headless')
# driver = webdriver.Chrome(options=options)
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
headers = {'User-Agent': "Mozilla/5.0 (X11; Linux x86_64; rv:99.0) Gecko/20100101 Firefox/99.0"}


@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""
    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()


def search_google(SEARCH_TERMS, DOMAINS_TO_DROP, NUM_RESULTS=100):
    """
    Searches google for a query and returns the top 10 results.
    """
    results = search(SEARCH_TERMS, lang="it", num_results=NUM_RESULTS)
    print("results", results)

    is_to_drop = lambda link: reduce(lambda a, b: a or b, [link.find(domain)>0 for domain in DOMAINS_TO_DROP])
    print("is_to_drop", is_to_drop)
    # print("[link.find(domain)>0 for domain in DOMAINS_TO_DROP]", [results.find(domain)>0 for domain in DOMAINS_TO_DROP])
    links = []
    for result in results:
        obj = re.findall('(\w+)://([\w\-\.]+)/(\w+).(\w+)', result)
        if len(obj) > 0:
            result = str(obj[0][0] + "://" + obj[0][1] + "/")
        if not is_to_drop(result):
            links.append(result)

    print("links", len(links), len(list(set(links))))
    return list(set(links))


def clean_code(code, cut_lenght=0, min_length=0):
    """
    Cleans code list of strings
    """
    for i in range(len(code)):
        try:
            for j in range(len(code[i])):
                code[i][j] = code[i][j].replace(".", "").replace(",", "").replace("-", "")
                code[i][j] = code[i][j].split("\n")[0]
                code[i][j] = code[i][j].split("\t")[0]
                code[i][j] = code[i][j].split("\\")[0]
                code[i][j] = code[i][j].replace(" ", "")
                if cut_lenght > 0:
                    code[i][j] = code[i][j][:cut_lenght]
        except:
            pass
    code = list(set([item for sublist in code for item in sublist]))
    code = [code[i] for i in range(len(code)) if code[i] != "" and len(code[i]) >= min_length]
    # sort list with first character in string as digit first
    # print("code", code)
    # code.sort(key=lambda x: int(x[0]))
    # print("sorted code", code)
    if code == []:
        code = ["no code found"]
    return code


def list_code(found_soup, Identification, follow_elements=5, follow_siblings=0):
    found = list(set([element.get_text().replace(":", " ").split(Identification)[1].strip().split(" ")[0]
                 for element in found_soup
                 if element.get_text().replace(":", " ").find(Identification) != -1]))
    try:
        for i in range(follow_elements):
            if len(found) != 0:
                found += list(set([element.get_text().replace(":", " ").split(Identification)[1].strip().split(" ")[i+1]
                  for element in found_soup
                  if element.get_text().replace(":", " ").find(Identification) != -1]))
    except:
        pass
    try:
        for i in range(follow_siblings):
            if len(found) != 0:
                found += list(set([element.next_sibling.replace(":", " ").split(Identification)[1].strip().split(" ")[i]
                                   for element in found_soup
                                   if element.get_text().replace(":", " ").find(Identification) != -1]))
                # print("next sibling", found)
    except:
        pass
    return found  # found

def scrape_subdomain(link, google_links):
    code = []
    try:
        r = requests.get(link, headers=headers)
        soup = BeautifulSoup(r.content, 'html.parser')
        href_links_child = [link.get('href') for link in soup.find_all('a', href=True)]
        href_links_child = list(set(href_links_child + [link.get('src') for link in soup.find_all(src=True)]))
        google_links = list(set([link for link in href_links_child if (link.find('google') != -1
                                                                       and link.find('maps') != -1)]))
        found_soup = [soup.find_all(tag) for tag in site_tags]
        for i in range(len(found_soup)):
            for j in range(len(Identification)):
                # print("tag[i]", site_tags[i], "Identification[j]", Identification[j])
                found_list = list_code(found_soup[i], Identification[j])
                if found_list != []:
                    code.append(found_list)
                    # print("found_list", found_list)
        try:
            code.append([soup.find(text=Identification).parent.parent.find_next_sibling().get_text()])
        except:
            pass
        code = list(set([item for sublist in code for item in sublist]))
    except:
        pass
    return code, google_links

def scrape_page(page):
    code = []
    google_links = []
    print("URL:", page)
    try:
        r = requests.get(page, headers=headers)
        soup = BeautifulSoup(r.content, 'html.parser')
        href_links = [link.get('href') for link in soup.find_all('a', href=True)]
        href_links.append(page)
        # print("find_all()", soup.find_all('font'))
        google_links = list(set([link for link in href_links if link.find('https://www.google.') == 0]))
        child_links = [link for link in href_links if link.find(page) == 0]
        child_links = list(set(child_links + [str(page + link[1:]) for link in href_links if link.find("/") == 0]))
        contatti_links = [link for link in child_links if link.find("contatt") != -1]
        print("child_links", len(child_links), "/", len(href_links), child_links)
        if contatti_links != []:
            print("contatti found and replaced", contatti_links)
            child_links = contatti_links
        else:
            print("no contatti page found")
        with tqdm_joblib(tqdm(desc=str("Subdomains of "+page), total=len(child_links))) as progress_bar:
            results = Parallel(n_jobs=JOBS_Subdomains)(delayed(scrape_subdomain)(page, google_links) for page in child_links)
        # maps = extraxt_maps_data(google_links, google_Identification, google_site_tags)
        # for link in google_links:
        #    print("Google maps:", googleDriver.scrape(link))
        for i in range(len(results)):
            code.append(results[i][0])
            google_links.extend(results[i][1])
        print("code", code)
        code = clean_code(code, cut_lenght=code_cut_lenght, min_length=min_code_length)
        print("code page", code)
        print("google_links", google_links)
        # remove everything but domain from link
        print("page", page.strip("https://").strip("www.").split(".")[0])
        page_dict[page.strip("https://").strip("www.").split(".")[0]] = {"URL": page,
                                                                         "code": code,
                                                                         "search_terms": SEARCH_TERMS,
                                                                         "google": google_links}
        print("\n")
    except:
        print("URL FAILED:", page, "\n")
        code.append(["URL FAILED"])
        code = clean_code(code, cut_lenght=code_cut_lenght, min_length=min_code_length)
    return page, code, google_links


def eval_codes(page_dict):
    # read known_subjects.xlsx and create a dataframe
    df = pd.read_excel("known_subjects.xlsx")
    print("df", df.columns)
    # print df with column "VAT"
    print("df", type(df['VAT'][1]))  # , df['VAT']

    count = 0
    for page in page_dict:
        print("page", page)
        try:
            found_bool = False
            page_dict[page]["in list"] = found_bool
            # print("page", page, page_dict[page.strip("https://").strip("www.").split(".")[0]]['code'],
            # page_dict[page.strip("https://").strip("www.").split(".")[0]].keys())
            for code in page_dict[page]["code"]:
                if not df.loc[df['VAT'] == code].empty:
                    print("code found!", df.loc[df['VAT'] == code], page)
                    found_bool = True
            page_dict[page]["in list"] = found_bool
            if found_bool:
                count += 1
        except:
            print("page", page, "failed", page_dict[page]["code"])

    print("total entries web", len(page_dict))
    print("no code found", len([page_dict[key] for key in page_dict if page_dict[key]["code"] == ['no code found']]))
    print("total entries df", len(df))
    print("entries found", count)
    print("entries w/o match", len(page_dict) -
          len([page_dict[key] for key in page_dict if page_dict[key]["code"] == ['no code found']]) - count)
    return page_dict

# def extraxt_maps_data(google_links, google_identification, google_tags):
#     for link in google_links:
#         try:
#             driver = webdriver.Chrome(options=options)
#             print("link", link)
#             print("google_identification", google_identification, "google_tags", google_tags)
#             driver.get(link)
#             driver.implicitly_wait(0.5)
#             print("driver.page_source", driver.page_source)
#             entry = driver.find_elements(by=By.CLASS_NAME, value="website")
#             print([a.text for a in entry])
#             driver.quit()
#             # html = requests.get(link, headers=headers).text
#             # soup = BeautifulSoup(html, "html.parser")
#             # print("soup", soup)
#             # found_soup = [soup.find_all(tag) for tag in google_tags]
#             # print("found_soup", found_soup)
#             # for i in range(len(found_soup)):
#             #     for Identification in google_identification:
#             #        found = list(set([element.get_text().split(Identification)[1].strip()
#             #                     for element in found_soup[i]
#             #                      if element.get_text().replace(":", " ").find(Identification) != -1]))
#             #        print("found", found)
#         except:
#             pass
#
#     return True  # found

# SEARCH_TERMS = "Company Italia"
SEARCH_TERMS = "tipografia trentino"
SEARCH_TERMS = "serigrafia trentino"
#SEARCH_TERMS = "carpenteria metallica trentino"  # falegnameria  #  carrozzeria
#SEARCH_TERMS = "segheria trentino"
#SEARCH_TERMS = "autofficina trentino"

NUM_RESULTS = 100
code_cut_lenght = 11
min_code_length = 10
Identification = ['P.I.', 'IVA', "I.V.A.", 'P.Iva', 'P. Iva', 'P.iva', 'PI', 'Codice Fiscale', 'C.F.', 'VAT', 'Pi']  # P.Iva
site_tags = ['h2', 'p', 'font', 'class', 'span', 'div', 'br', 'iframe', 'td']  # , 'a'
google_Identification = ['Adresse:', 'Website:', 'Telefon:']
google_site_tags = ['button']
JOBS_Sites = 2
JOBS_Subdomains = 4

# open DOMAINS_TO_DROP from a file
with open("DOMAINS_TO_DROP.txt", "r") as f:
    DOMAINS_TO_DROP = [line.strip() for line in f.readlines()]


# check if file 'link_list_' + SEARCH_TERMS.replace(" ", "_") + '.json' exists
if os.path.isfile('link_list_' + SEARCH_TERMS.replace(" ", "_") + '.json'):
    print("file 'link_list_" + SEARCH_TERMS.replace(" ", "_") + ".json' exists")
    with open('link_list_' + SEARCH_TERMS.replace(" ", "_") + '.json', 'r') as f:
        with open('link_list_' + SEARCH_TERMS.replace(" ", "_") + '.json', "r") as f:
            page_url_list = [line.strip() for line in f.readlines()]
            print("page_url_list", page_url_list)
else:
    print("file 'link_list_" + SEARCH_TERMS.replace(" ", "_") + ".json' does not exist")
    print("searching for", NUM_RESULTS, "links in google:", SEARCH_TERMS)
    page_url_list = search_google(SEARCH_TERMS, DOMAINS_TO_DROP, NUM_RESULTS=NUM_RESULTS)
    print("page_url_list", page_url_list)
    with open('link_list_' + SEARCH_TERMS.replace(" ", "_") + '.json', "w") as f:
        for line in page_url_list:
            f.write(line + "\n")
    print("page_url_list saved to file", 'link_list_' + SEARCH_TERMS.replace(" ", "_") + '.json')
# open page_url_list from a file
with open('link_list_' + SEARCH_TERMS.replace(" ", "_") + '.json', "r") as f:
    page_url_list = [line.strip() for line in f.readlines()]
    print("page_url_list", page_url_list)


# page_url_list = ["https://www.e3ssport.it/", "https://franzmagazine.com/impressum/",
#                  "https://www.elenchitelefonici.it/", "https://www.moschiniadv.com/contact",
#                  "http://www.starcoppe.it/", "https://trento.impacthub.net/",
#                  "https://www.cfpackaging.eu/", "https://www.alregalin.com/",
#                  "http://www.serigrafiamori.it/", "https://serigamma.it/", "https://www.artribune.com/",
#                  ]

# page_url_list = ["https://www.transkom.it/transkom.aspx"]

page_dict = {}
# results = Parallel(n_jobs=6)(delayed(scrape_page)(page) for page in tqdm(page_url_list))
with tqdm_joblib(tqdm(desc="Scraper", total=len(page_url_list))) as progress_bar:
    results = Parallel(n_jobs=JOBS_Sites)(delayed(scrape_page)(page) for page in page_url_list)

for i in range(len(results)):
    page_dict[results[i][0].strip("https://").strip("www.").split(".")[0]] = {"URL": results[i][0],
                                                                     "code": results[i][1],
                                                                     "search_terms": SEARCH_TERMS,
                                                                     "google": results[i][2]}

[print(key, page_dict[key]) for key in page_dict]
# count entry in dict where code is not empty
print("\n", len([page_dict[key] for key in page_dict if page_dict[key]["code"] != []]))
# save to json
with open(str('search_result_' + SEARCH_TERMS.replace(" ", "_") + '.json'), 'w') as outfile:
    json.dump(page_dict, outfile)
print("saved to json")

with open('search_result_' + SEARCH_TERMS.replace(" ", "_") + '.json', 'r') as f:
    page_dict = json.load(f)
    f.close()

[print(key, page_dict[key]) for key in page_dict]
# count entry in dict where code is not empty
print("\n", len([page_dict[key] for key in page_dict if page_dict[key]["code"] != []]))
# count 'no code found' entries in page_dict['code']
print("no code found", len([page_dict[key] for key in page_dict if page_dict[key]["code"] == ['no code found']]))

page_dict = eval_codes(page_dict)

with open(str('search_result_' + SEARCH_TERMS.replace(" ", "_") + '.json'), 'w') as outfile:
    json.dump(page_dict, outfile)
print("saved to json")

df = pd.DataFrame.from_dict(page_dict, orient='index')
df = df[['URL', 'code', 'search_terms', 'in list', 'google']]
# save to excel
df.to_excel("search_result_" + SEARCH_TERMS.replace(" ", "_") + ".xlsx")
print("saved to excel", df.columns)


