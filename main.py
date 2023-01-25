# -*- coding: utf-8 -*-
import requests, bs4, sys, os, time, json, re, random, ast
from multiprocessing import Process, Manager
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime
from requests.structures import CaseInsensitiveDict
from selenium import webdriver
import openpyxl
import CollectUrl2CSV
import CollectDetail2Excel
import DownloadMedias
import logging
import numpy as np

logging.root.handlers = []
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO , filename='tedlog.log')

# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

TED_Talk_URLs = 'TED_Talk_URLs.txt'
TED_Talk_Excel = 'TED_Talk.xlsx'
TED_Talk_CSV = 'TED_Talk.csv'
TedDataFolder = 'TED_DataBase'
if __name__ == '__main__':
    skip_collect_url = input("skip collect url from ted website? (1:true 0:false) ")
    # 1. collect data from ted web
    if skip_collect_url == '0' :
        time__  =  time.time()
        collect_url = input("collect which url? (all/any/www.ted.com.xxxx) ")
    
        logging.info("=============start collect urls==============")
        if collect_url == 'all':
            CollectUrl2CSV.CollectAllUrls()
        elif collect_url == 'any':
            #load any
            logging.info("use txt to collect")
        elif collect_url.startswith('https://www.ted.com/talks/'):
            CollectUrl2CSV.CollectSpeUrl(collect_url)

        logging.info("=============start collect details==============")
        f = open(TED_Talk_URLs, 'r+')
        urllines = [line for line in f.readlines()]
        f.close()

        urllines_filter =[] 
        if os.path.exists(TED_Talk_Excel):
            df  =  pd.read_excel(TED_Talk_Excel)
            # tmplist = ~pd.Series(urllines).isin(df['url__webpage'])
            # if tmplist.all():
            #     urllines_filter = urllines[tmplist]
            urllines_filter = [line for line in urllines if line not in df['url__webpage'].tolist()]
        else:
            urllines_filter = urllines

        if len(urllines_filter) != 0 :
            csv_list_ = []

            with  Manager()  as manager:
                csv_list = manager.list()    # SPECIAL variable - can be used only locally.
                Processess = []
                concurreny_count  =  10
                urls_  =  [urllines_filter[(i* (len(urllines_filter)//concurreny_count)):((i+1)* (len(urllines_filter)//concurreny_count))]    for i in range(concurreny_count)]

                leftovers  =  urllines_filter[(concurreny_count * (len(urllines_filter)//concurreny_count))  :  len(urllines_filter)]
                for i in range(len(leftovers)):    
                    urls_[i] += [leftovers[i]]
                #none_empty_list = [i for i in urls_ if i]
                for  (id_,urls__)  in enumerate(urls_):
                    time.sleep(random.randint(0,900)/1000)     # Randomly wait for 0-0.9 seconds.
                    p = Process(target=CollectDetail2Excel.download, args=(urls__,id_,csv_list))
                    Processess.append(p)
                    p.start()

                # block until all the threads finish (i.e. block until all **download** function calls finish)
                for t in Processess:    
                    time.sleep(random.randint(0,900)/1000)     # Randomly wait for 0-0.9 seconds.
                    t.join()

                csv_list_ = list(csv_list)

            # Creating DataFrame.
            df  =  pd.DataFrame(csv_list_)

            # Sort - most popular first.
            df = df.sort_values("view_count", ascending=False)

            # Saving.
            df.to_excel(TED_Talk_Excel, encoding='utf-8', index=False,engine='openpyxl')
            df.to_csv(TED_Talk_CSV, index=False, encoding='utf-8')

        else:
            logging.info('have not ted web url to download!!')

        logging.info('collect cost: %f',round(time.time()  -  time__))
    
    # 2. start download media files   

    if not os.path.exists(TedDataFolder):    
        os.makedirs(TedDataFolder) 
    df  =  pd.read_excel(TED_Talk_Excel)

    time__  =  time.time()
    concurreny_count  =  10
    DownloadMedias.download_in_parallel(concurreny_count, df, TedDataFolder)

    logging.info('download cost: %f',round(time.time()  -  time__))




