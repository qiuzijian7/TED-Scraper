
import requests, bs4, sys, os, time, json, re, random, ast
from multiprocessing import Process, Manager
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime
from requests.structures import CaseInsensitiveDict

ted_talk_url_file = 'TED_Talk_URLs.txt'

if os.path.exists(ted_talk_url_file):
    print('delete file',ted_talk_url_file)
    os.remove(ted_talk_url_file)

urls = []
page_number=0
UserAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
# 防止 http连接太多没有关闭导致 Max retries exceeded
requests.adapters.DEFAULT_RETRIES = 5
s = requests.session()
s.keep_alive = False
while 1:
    page_number += 1

    res  =  s.get("https://www.ted.com/talks?sort=popular&page=" + str(page_number), headers = {'User-Agent':UserAgent })
   
    soup = bs4.BeautifulSoup(res.text)
    e = soup.select("div.container.results div.col")
    print(str(soup))
    
    if len(e) == 0:    
        print('can not find ted resource')
        break    # No more videos.
    
    for  u  in e:
        urls.append(  "https://www.ted.com" + u.select("div.media__image a.ga-link")[0].get("href")  )


# Saving.
f = open(ted_talk_url_file, 'w')
f.write('\n'.join(urls))
f.close()