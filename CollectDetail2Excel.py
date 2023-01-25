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
import logging
'''  IGNORING WARNINGS  '''
import warnings
warnings.filterwarnings('ignore')


'''  NOTEBOOK DISPLAY SETTINGS  '''
pd.options.display.max_rows = 10000    # None
pd.options.display.max_columns = None

pd.set_option('display.max_colwidth', -1)
pd.set_option('display.expand_frame_repr', False)
#from IPython.core.display import display, HTML
#display(HTML("<style>.container { width:95% !important; }</style>"))

#os.chdir('D:/TEDData/')1

time__  =  time.time()

f = open('TED_Talk_URLs.txt', 'r+')
urllines = [line for line in f.readlines()]
f.close()

requests.adapters.DEFAULT_RETRIES = 5
s = requests.session()
s.keep_alive = False

UserAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
opt = webdriver.ChromeOptions()   #创建浏览
opt.add_argument("--headless")  #无窗口模式


def download(urls, id_, csv_list):
    logging.info('download start:')
    for count, url in enumerate(urls):
        logging.info(url)
        def get_transcript(name,url,language):
           # transcriptUrl = 'https://www.ted.com/graphql?operationName=Transcript&variables={"id":"alexis_nikole_nelson_a_flavorful_field_guide_to_foraging","language":"zh-cn"}&extensions={"persistedQuery":{"version":1,"sha256Hash":"906b90e820733c27cab3bb5de1cb4578657af4610c346b235b4ece9e89dc88bd"}}'
            url = url.strip().replace('\t', '').replace('\n', ' ')
            transcriptUrl = 'https://www.ted.com/graphql?operationName=Transcript&variables={"id":"' + name + '","language":"'+language+'"}&extensions={"persistedQuery":{"version":1,"sha256Hash":"906b90e820733c27cab3bb5de1cb4578657af4610c346b235b4ece9e89dc88bd"}}'
            # logging.info('transcript:%s',transcriptUrl)
            # headers = CaseInsensitiveDict()
            # headers["User-Agent"] = UserAgent
            # headers["Accept"] = "*/*"
            # headers["Accept-Language"] = "en-US,en;q=0.5"
            # headers["Accept-Encoding"] = "gzip, deflate, br"
            # headers["Referer"] = url+'/transcript'
            # headers["content-type"] = "application/json"
            # headers["client-id"] = "Zenith production"
            # headers["x-operation-name"] = "Transcript"
            # resp = s.get(transcriptUrl, headers=headers)
            # #logging.info(resp.content.decode())
            # if 'errors' in resp.text:
            #     logging.info('error transcript: %s',transcriptUrl)
            # src_str =''
            # try:
            #     data = json.loads(resp.text)
            #     src_str = data['data']['translation']['paragraphs']
            # except json.decoder.JSONDecodeError as e:
            #     logging.info("An error occurred while parsing the JSON transcript:%s,%s", e,url)
             
            return transcriptUrl
        
        def get_video_audio_url(url):    
            try:  
                driver = webdriver.Chrome(options=opt)  #创建浏览器对象
                driver.get(url) #打开网页
                # driver.maximize_window()   #最大化窗口
                time.sleep(1)     #加载等待
            except Exception as e:
                logging.info("An error occurred while navigating to the webpage:%s", e)
                return '',''

            try:
                driver.find_element("xpath",'//div/Button[@class="rounded-sm text-sm font-medium"]/div/div[contains(text(),"Share")]').click()
            except:
                logging.info("Share button not found!")
            try:
                element_video = driver.find_element("xpath",'//a[starts-with(@href,"https://download.ted.com/products/")]')
                video_url = element_video.get_attribute('href')
            except:
                logging.info("element_video not found in: %s",url)
                video_url = ''
            try:
                element_audio = driver.find_element("xpath",'//a[starts-with(@href,"https://download.ted.com/talks/")]')
                audio_url = element_audio.get_attribute('href')
            except:
                logging.info("element_audio not found in: %s",url)
                audio_url = ''
            driver.close()
            return video_url,audio_url

        def get__json_obj(url):
            html = ''
            while html=='':
                try:
                    res = s.get(url.strip(), headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36' })
                    html = res.text
                    #start_index  =  html.find('<script data-spec="q">q("talkPage.init",')
                    start_index  =  html.find('<script id="__NEXT_DATA__" type="application/json">')
                    end_index    =  html[start_index:].find('</script>')
                    script_tag   =  html[start_index: start_index + end_index]
                    json_obj  =  script_tag[len('<script id="__NEXT_DATA__" type="application/json">'):]
                    return json_obj
                except:
                    logging.info("Connection refused by the server.. %s",url)
                    time.sleep(5)
                    logging.info("Was a nice sleep, now let me continue...%s",url)
                    continue
        
        json_obj  =  get__json_obj(url)
        
        if not json_obj:
            count=0
            while  count < 3:    # Check 3 more times
                json_obj  =  get__json_obj(url)
                count += 1
                if json_obj:    
                    break

        if not json_obj:    
            logging.info('url failed: %s', url);
            continue;
        else:  
            logging.info('url success: %s', url);        
            metadata = json.loads(json_obj)["props"]
  
    
        def get_value(l, m=metadata):
            for i in l:
                try:    m = m[i]
                except: return ''
            return m


        def html_to_text(html):
            if str(html)!='nan':
                soup = BeautifulSoup(html)
                return soup.get_text()
            else: return html

 
        if get_value(["pageProps","action","code"], metadata) == 404:
            logging.info('page 404:%s', url)
            continue

        d = dict()
        # ALL THESE SPEAKER ATTRIBUTES CORRESPOND TO THE 1ST SPEAKER.
        d["talk__id"]  =  get_value(["pageProps","videoData","id"], metadata)
        d["talk__name"]  =  get_value(["pageProps","videoData","title"], metadata)
        d["talk__description"]  =  get_value(["pageProps","videoData","description"], metadata)


        d["view_count"]  =  get_value(["pageProps","videoData","viewedCount"], metadata) or 0
        #d["comment_count"]  =  get_value(["pageProps","comments", "count"], metadata)


        d["duration"]  =  get_value(["pageProps","videoData","duration"], metadata)    # In seconds.

        slug = get_value(["pageProps","videoData","slug"], metadata)
        language  =  get_value(["language"], metadata)
 

        playerData = get_value(["pageProps","videoData","playerData"], metadata)
        playerDataMetadata = dict()
        try:
            playerDataMetadata = json.loads(playerData)
        except json.decoder.JSONDecodeError as e:
            logging.info("An error occurred while parsing the JSON playerData:%s,%s", e,url)

       
        #url__transcript  =  url + "/transcript?language=" + language
        d["slug"]  =  slug
        d["transcript_zh-cn"]  =  get_transcript(slug,url,'zh-cn')
        d["transcript_en"]  =  get_transcript(slug,url,'en')


        d["video_type_name"]  =  get_value(["pageProps", "videoData", "tpye","name"], metadata)    # One of:  TED Stage Talk, TEDx Talk, TED-Ed Original, TED Institute Talk, Best of Web, Original Content, TED Salon Talk (partner), Custom sponsored content
        d["number_of__speakers"]  =  len(get_value(["pageProps", "videoData","speakers","nodes"], metadata) or "")

        # First speaker details.                 
        d["speaker__name"]  =  get_value(["pageProps", "videoData","presenterDisplayName"], metadata)   
        d["speaker__description"]  =  get_value(["pageProps", "videoData","speakers","nodes", 0,"description"], metadata)   

        # Recorded and Published time.
        temp  =  get_value(["pageProps", "videoData", "recordedOn"], metadata)
        d["recording_date"]  =  temp  if temp==None  else temp[:10]
        d["published_timestamp"]  =  get_value(["pageProps", "videoData", "publishedAt"], metadata)

        # Tags
        topicsLen = len(get_value(["pageProps", "videoData", "topics","nodes"], metadata))

        d["talks__tags"]  =  get_value(["targeting", "tag"], playerDataMetadata)
        d["number_of__tags"]  =  topicsLen or ""
        d["event"]  =  get_value(["event"], playerDataMetadata)
        

        d["language"]  =  language
        d["native_language"]  =  get_value(["nativeLanguage"], playerDataMetadata)


        # URLs.
        d["url__webpage"]  =  url #get_value(["url"], metadata)
        url__video, url__audio =get_video_audio_url(url)
        d["url__audio"]  =  url__audio
        d["url__video"]  =  url__video
        d["url__h264"]  =  get_value(["resources", "h264",0,"file"], playerDataMetadata)
        d["url__bitrate"]  =  get_value(["resources", "h264",0, "bitrate"], playerDataMetadata)
        d["url__photo__talk"]  =  get_value(["pageProps", "videoData","primaryImageSet", 0, "url"], metadata)#none      
        csv_list.append(d)




