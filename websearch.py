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
        for j in range(len(code[i])):
            code[i][j] = code[i][j].replace(".", "").replace(",", "").replace("-", "")
            code[i][j] = code[i][j].split("\n")[0]
            code[i][j] = code[i][j].split("\t")[0]
            code[i][j] = code[i][j].split("\\")[0]
            code[i][j] = code[i][j].replace(" ", "")
            if cut_lenght > 0:
                code[i][j] = code[i][j][:cut_lenght]
    code = list(set([item for sublist in code for item in sublist]))
    code = [code[i] for i in range(len(code)) if code[i] != "" and len(code[i]) >= min_length]
    if code == []:
        code = ["no code found"]
    return code


def list_code(found_soup, Identification):
    found = list(set([element.get_text().replace(":", " ").split(Identification)[1].strip().split(" ")[0]
                 for element in found_soup
                 if element.get_text().replace(":", " ").find(Identification) != -1]))
    try:
        if len(found) != 0:
            found += list(set([element.get_text().replace(":", " ").split(Identification)[1].strip().split(" ")[1]
              for element in found_soup
              if element.get_text().replace(":", " ").find(Identification) != -1]))
    except:
        pass
    return found  # found


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


SEARCH_TERMS = "autofficina trentino"  # falegnameria  #  carrozzeria
DOMAINS_TO_DROP = ["paginegialle.it", "virgilio.it", "paginebianche.it", "https://it.indeed.com/"]
NUM_RESULTS = 100
code_cut_lenght = 11
min_code_length = 10
# Identification = ['P. IVA', 'P.IVA', 'P.I.', 'P.I. e C.F.', 'P.Iva', 'P.Iva', 'P.Iva:', 'C.F.', 'P.iva', 'P.IVA:',
#                   'Codice Fiscale:', 'VAT', 'VAT:', 'IVA', 'IVA:']  # 'P. IVA', 'P.IVA'
Identification = ['P.I.', 'IVA', 'P.Iva', 'P. Iva', 'P.iva', 'PI', 'Codice Fiscale', 'C.F.', 'VAT']  # P.Iva
site_tags = ['h2', 'p', 'font', 'class', 'span', 'div', 'br', 'iframe']  # , 'a'
google_Identification = ['Adresse:', 'Website:', 'Telefon:']  # P.Iva
google_site_tags = ['button']
# create a thread pool
# executor = ThreadPoolExecutor(20)


# page_url_list = search_google(SEARCH_TERMS, DOMAINS_TO_DROP, NUM_RESULTS=NUM_RESULTS)
# print("page_url_list", page_url_list)

page_url_list = list(set(['https://www.carrozzeriaofficinapedrotti.it/', 'https://carrozzeriagildo.it/', 'https://www.ravinacar.it/servizi/carrozzeria-officina-trento/', 'https://www.carrozzeriacrepaz.com/', 'https://autosarca.it/', 'https://www.carrozzerianicolinitrento.com/', 'https://www.carrozzeriagamma.com/', 'https://autosarca.it/', 'https://www.carrozzerianicolinitrento.com/', 'https://www.carrozzeriagamma.com/']))
# page_url_list = ['https://www.reteimprese.it/', 'https://www.touringclub.it/', 'https://www.officinamatta.com/', 'https://www.centro-assistenza.info/', 'https://www.youdriver.com/', 'https://nicelocal.it/', 'https://italiarecensioni.com/', 'https://trova-aperto.it/', 'https://officinapucher.it/', 'https://autosarca.it/', 'https://carrozzeria-benedetti-trento.it/', 'https://www.cgrrevisioni.it/', 'https://www.officinaatb.it/', 'https://www.carrozzeriaofficinapedrotti.it/', 'https://www.ederofficinanaturale.it/', 'https://www.franchinigomme.it/', 'https://www.smanapp.com/', 'https://www.google.com/', 'https://www.dorigoni.com/', 'http://www.autofficinaorlando.it/', 'https://www.revisionionline.com/', 'https://www.motogp-cashback.com/', 'https://www.nuovaofficinatrento.com/', 'https://www.officinapanizza.it/', 'https://www.ricercaimprese.com/', 'https://it.indeed.com/', 'https://firmania.it/', 'https://makcostruzioni.it/', 'https://www.misterimprese.it/', 'https://anffas.tn.it/', 'https://www.pagineweb.it/', 'https://cumaps.net/', 'https://www.officinaautosembenotti.it/', 'https://autofficinabozzetta.it/', 'https://www.autolucca.tn.it/', 'https://autofficinabiasioligraziano.it/', 'https://www.giornaletrentino.it/', 'https://www.meccanico.info/', 'https://www.visitvaldisole.it/', 'http://www.autofficinatrento.it/', 'https://www.autofficina24.com/', 'https://www.revisione24.it/', 'https://www.drivercenter.eu/', 'https://www.gruppoalpin.it/', 'https://www.officinasaurini-cles.it/', 'https://www.officinalorenzo.com/', 'https://www.oraridiapertura24.it/', 'https://www.soccorsostradalepedrotti.it/', 'https://officinadambrosio.it/', 'https://www.autoservicetrento.it/', 'https://www.informazione-aziende.it/', 'https://officinacarloni.it/', 'https://it.foursquare.com/', 'http://www.cercattivita.net/', 'https://www.aposto.it/', 'http://vianinigpl.it/', 'https://www.officinalever.com/', 'http://www.autogocciadoro.it/', 'https://www.visitpinecembra.it/', 'https://www.starofservice.it/', 'https://forum.clubalfa.it/', 'https://www.tuugo.it/', 'https://www.trentinotrasporti.it/', 'https://www.helpmecovid.com/', 'https://it.geoview.info/', 'https://carrozzeriagildo.it/', 'https://autofficinamottes.it/', 'https://unicommercialista.it/', 'https://www.autoanaunediclauserelio.it/', 'https://trentinosoccorso.it/', 'http://www.officinabolner.it/', 'https://www.ederofficinanaturale.eu/', 'http://www.reprocar.it/', 'https://www.carrozzeriaofficinabosetti.it/', 'https://www.topcarcles.it/', 'https://www.tecnodiesel.it/', 'https://www.scaletauto.com/', 'https://www.centri-assistenza.com/', 'https://lofficinadiflaviozamboni.it/', 'https://www.paginemail.it/', 'http://www.autofficinacielle.it/', 'https://local.motorionline.com/', 'https://autofficine.top10posti.it/', 'https://www.officinasmilecar.it/', 'http://www.elettrautorigo.com/', 'https://www.pitstopadvisor.com/', 'https://www.instagram.com/', 'https://www.autoofficina.it/', 'https://www.fellincar.it/']
# page_url_list = ['http://www.autogocciadoro.it/', 'http://www.autofficinacielle.it/', 'https://www.nuovaofficinatrento.com/', 'https://mapsus.net/', 'https://mapcarta.com/', 'https://local.motorionline.com/', 'https://officinadambrosio.it/', 'https://www.revisionionline.com/', 'https://www.trentinotrasporti.it/', 'https://unicommercialista.it/', 'https://www.autoofficina.it/', 'https://www.autoservicetrento.it/', 'https://www.tuugo.it/', 'https://www.soccorsostradalepedrotti.it/', 'https://carrozzeriagildo.it/', 'https://autofficinatoss.it/', 'https://www.visitpinecembra.it/', 'https://www.motogp-cashback.com/', 'https://anffas.tn.it/', 'https://www.helpmecovid.com/', 'http://www.officinabolner.it/', 'https://www.autofficina24.com/', 'https://www.officinasmilecar.it/', 'https://www.officinalever.com/', 'https://www.touringclub.it/', 'https://www.officinaatb.it/', 'http://www.elettrautorigo.com/', 'http://www.cercattivita.net/', 'https://www.aposto.it/', 'https://italiarecensioni.com/', 'https://autofficinabozzetta.it/', 'https://www.instagram.com/', 'https://www.revisione24.it/', 'https://it.geoview.info/', 'https://www.trapasso.eu/', 'https://www.ederofficinanaturale.eu/', 'https://www.officinalorenzo.com/', 'https://trova-aperto.it/', 'http://www.autofficinaorlando.it/', 'https://autofficinamottes.it/', 'https://autofficinabiasioligraziano.it/', 'https://www.scaletauto.com/', 'https://www.smanapp.com/', 'http://www.reprocar.it/', 'https://www.autolucca.tn.it/', 'https://officinacarloni.it/', 'https://www.officinaautosembenotti.it/', 'https://www.centri-assistenza.com/', 'https://www.ricercaimprese.com/', 'https://firmania.it/', 'https://www.fellincar.it/', 'https://carrozzeria-benedetti-trento.it/', 'https://www.demattesnc.it/', 'https://www.officinapanizza.it/', 'https://lofficinadiflaviozamboni.it/', 'https://officinapucher.it/', 'https://www.reteimprese.it/', 'https://www.gruppoalpin.it/', 'https://www.meccanico.info/', 'https://www.autoanaunediclauserelio.it/', 'https://www.informazione-aziende.it/', 'https://www.oraridiapertura24.it/', 'https://trentinosoccorso.it/', 'https://www.pitstopadvisor.com/', 'https://it.indeed.com/', 'https://www.giornaletrentino.it/', 'https://autofficine.top10posti.it/', 'https://it.foursquare.com/', 'https://automaxlavis.com/', 'http://vianinigpl.it/', 'https://www.officinamatta.com/', 'https://www.google.com/', 'https://www.pagineweb.it/', 'https://www.drivercenter.eu/', 'https://forum.clubalfa.it/', 'https://makcostruzioni.it/', 'https://www.dorigoni.com/', 'https://www.misterimprese.it/', 'https://www.carrozzeriaofficinapedrotti.it/', 'https://autosarca.it/', 'https://www.centro-assistenza.info/', 'https://www.officinasaurini-cles.it/', 'https://www.franchinigomme.it/', 'https://www.starofservice.it/', 'https://www.secondavetrina.it/', 'https://www.carrozzeriaofficinabosetti.it/', 'https://www.tecnodiesel.it/', 'http://www.autofficinatrento.it/', 'https://www.ederofficinanaturale.it/']


# load jason search_result.json
# with open('search_result.json', 'r') as data_file:
#     page_dict = json.load(data_file)
#     print("page_dict", page_dict)

# give all entries of page_dict where the key is code and the string is 'no code found'
# page_url_list = []
# for key, value in page_dict.items():
#    if value['code'] == ['no code found']:
#        page_url_list.append(value['URL'])

# print("URLS", page_url_list)

page_dict = {}
for page in page_url_list:
    # add entry page with page to page_dict for each page
    code = []
    print("URL:", page)
    try:
        r = requests.get(page, headers=headers)
        soup = BeautifulSoup(r.content, 'html.parser')
        href_links = [link.get('href') for link in soup.find_all('a', href=True)]
        # print("find_all()", soup.find_all('font'))
        google_links = list(set([link for link in href_links if link.find('https://www.google.') == 0]))
        print("google_links", google_links)
        child_links = [link for link in href_links if link.find(page) == 0]  #
        child_links = list(set(child_links + [str(page + link[1:]) for link in href_links if link.find("/") == 0]))
        contatti_links = [link for link in child_links if link.find("contatt") != -1]
        print("child_links", len(child_links), "/", len(href_links), child_links)
        if contatti_links != []:
            print("contatti found and replaced", contatti_links)
            child_links = contatti_links
        for link in child_links:
            try:
                r = requests.get(link, headers=headers)
                soup = BeautifulSoup(r.content, 'html.parser')
                href_links_child = [link.get('href') for link in soup.find_all('a', href=True)]
                href_links_child = list(set(href_links_child + [link.get('src') for link in soup.find_all(src=True)]))
                google_links = list(set(google_links +
                                        [link for link in href_links_child if (link.find('google') != -1
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
                except: pass
            except: pass
        print("google_links", google_links)
        # maps = extraxt_maps_data(google_links, google_Identification, google_site_tags)
        # for link in google_links:
        #    print("Google maps:", googleDriver.scrape(link))
        # print("code", code, "code[0]", code[0], "code[0][0]", code[0][0], "len", len(code))
        print("code", code)
        code = clean_code(code, cut_lenght=code_cut_lenght, min_length=min_code_length)
        print("code page", code)
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

[print(key, page_dict[key]) for key in page_dict]
# count entry in dict where code is not empty
print("\n", len([page_dict[key] for key in page_dict if page_dict[key]["code"] != []]))
# save to json
with open('search_result.json', 'w') as outfile:
    json.dump(page_dict, outfile)
print("saved to json")


# data = extraxt_maps_data(google_links=["https://www.google.it/maps/place/Autosarca+Snc+Di+Frioli+Gianfranco+E+C.+Snc/@46.0486835,10.9523088,17z/data=!4m5!3m4!1s0x47826c386f7241a5:0x37f2e720a88b8a2f!8m2!3d46.0486722!4d10.95451"],
#                   google_identification=google_Identification,
#                   google_tags=google_site_tags,
#                   )
# print("extract_maps_data", data)
# breakpoint()
# s = requests.Session()
# s.auth = ('user', 'pass')
# s.headers.update({'x-test': 'true'})
# # both 'x-test' and 'x-test2' are sent
# s.get('https://httpbin.org/headers', headers={'x-test2': 'true'})


# read known_subjects.xlsx and create a dataframe
# df = pd.read_excel("known_subjects.xlsx")
# print("df", df.columns)
# #
# # print df with column "VAT"
# print("df", df['VAT'], type(df['VAT'][1]))
#
#
# # check if code is in the dataframe 'VAT'
# print("df", df.loc[df['VAT'] == code])
