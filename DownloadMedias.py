import requests, bs4, sys, os, time, json, re, random, ast
from multiprocessing import Process, Manager
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime
from requests.structures import CaseInsensitiveDict
from selenium import webdriver
import openpyxl

#df  =  pd.read_excel('TED_Talk.xlsx')
#df.shape[0], df[~df['transcript_en'].isna()].shape[0]
#df.shape[0], df[~df['transcript_zh-cn'].isna()].shape[0]

# Look at the first row.
#df.iloc[0]
#pd.DataFrame([[col, df[col].nunique(), df[col].isna().sum()]  for  col  in df], columns = ['Column Name', 'Unique Count', 'Missing Count'])

# Accessing a list.
#x = ast.literal_eval(df['talks__tags'].iloc[0])
#x, x[-2]

# Accessing a list of dictionaries.
#x = ast.literal_eval(df['related_talks'].iloc[0])
#x, x[0], x[1]['slug']

# Accessing a dictionary of dictionary.
#x = ast.literal_eval(df['url__subtitled_videos'].iloc[0])
#x, x['az'], x['az']['name']

def download_in_parallel(concurreny_count, urls, fn, folder, extension):
    Processes = []

    urls_  =  [urls[(i* (len(urls)//concurreny_count)):((i+1)* (len(urls)//concurreny_count))]    for i in range(concurreny_count)]

    leftovers  =  urls[(concurreny_count * (len(urls)//concurreny_count))  :  len(urls)]
    for i in range(len(leftovers)):    
        urls_[i] += [leftovers[i]]


    for  (id_,urls__)  in enumerate(urls_):
        p = Process(target=fn, args=(urls__,id_, folder, extension))
        Processes.append(p)
        p.start()


    # block until all the threads finish (i.e. block until all function_x calls finish)
    for t in Processes:    
        t.join()



def download_any(urls, id_, folder, extension):    # To download any file.
    for  (count, url) in enumerate(urls):
        url,filename = url
        try:                      
            urllib.request.urlretrieve(url, folder+'/'+str(filename)+extension)
        except Exception as e:    
            print('\n\n\n', url, '\n', e)

df  =  pd.read_excel('TED_Talk.xlsx')

# DOWNLOADING 'url__photo__talk'. Saving by 'talk__id'.

urls  =  df[['url__photo__talk', 'talk__id']].dropna().values.tolist()

folder, extension  =  'PHOTO__TALK', '.jpg'
if not os.path.exists(folder):    
    os.makedirs(folder)


time__  =  time.time()

concurreny_count  =  100
download_in_parallel(concurreny_count, urls, download_any, folder, extension)

print(round(time.time()  -  time__))

# DOWNLOADING 'url__photo__speaker'. Saving by 'speaker__id'.

urls  =  [[i[0], int(i[1])]  for i in df[['url__photo__speaker', 'speaker__id']].dropna().values.tolist()]

folder, extension  =  'PHOTO__SPEAKER', '.jpg'
if not os.path.exists(folder):    os.makedirs(folder)


time__  =  time.time()

concurreny_count  =  100
download_in_parallel(concurreny_count, urls, download_any, folder, extension)

print(round(time.time()  -  time__))

# DOWNLOADING 'url_audio'. Saving by 'talk__id'.

urls  =  df[['url__audio', 'talk__id']].dropna().values.tolist()

folder, extension  =  'AUDIO', '.mp3'
if not os.path.exists(folder):    
    os.makedirs(folder)


time__  =  time.time()

concurreny_count  =  100
download_in_parallel(concurreny_count, urls, download_any, folder, extension)

print(round(time.time()  -  time__))

# DOWNLOADING 'url_video'. Saving by 'talk__id'.

s = df[['url__video', 'talk__id']].dropna().values.tolist()

def get_url(i):    # Because sometime 'high' is None.
    if ast.literal_eval(i)['high']:        
        return ast.literal_eval(i)['high']
    elif ast.literal_eval(i)['medium']:   
        return ast.literal_eval(i)['medium']
    else:                                  
        return ast.literal_eval(i)['low']

urls = [[get_url(i[0]), i[1]]  for i in s]

folder, extension  =  'VIDEO', '.mp4'
if not os.path.exists(folder):    
    os.makedirs(folder)



time__  =  time.time()

concurreny_count  =  8
download_in_parallel(concurreny_count, urls, download_any, folder, extension)

print(round(time.time()  -  time__))