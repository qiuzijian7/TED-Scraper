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
from retrying import retry
from tqdm import tqdm
import logging
from pysrt import SubRipItem, SubRipFile
import math
from urllib.request import urlopen

UserAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
requests.adapters.DEFAULT_RETRIES = 5
s = requests.session()
s.keep_alive = False

def transcript2src(dst_path,transcriptUrl):
    s_utf8 = dst_path #dst_path.encode('utf-8')
    logging.info('transcript:%s',transcriptUrl)
    headers = CaseInsensitiveDict()
    headers["User-Agent"] = UserAgent
    headers["Accept"] = "*/*"
    headers["Accept-Language"] = "en-US,en;q=0.5"
    headers["Accept-Encoding"] = "gzip, deflate, br"
    #headers["Referer"] = url+'/transcript'
    headers["content-type"] = "application/json"
    headers["client-id"] = "Zenith production"
    headers["x-operation-name"] = "Transcript"
    resp = s.get(transcriptUrl, headers=headers)
    #logging.info(resp.content.decode())
    if 'errors' in resp.text:
        logging.info('error transcript: %s',transcriptUrl)
        return

    try:
        transcript = json.loads(resp.text)
        if not transcript['data']['translation']:
            return
    except json.decoder.JSONDecodeError as e:
        logging.info("An error occurred while parsing the JSON transcript:%s, %s, %s", e,resp.text,transcriptUrl)
        return
            
    subs = SubRipFile()
    cnt = 1
    pararaphs = transcript["data"]["translation"]["paragraphs"]
    for i,paragraph in enumerate(pararaphs):
        # Get the cues list
        cues = paragraph["cues"]
        cues_next = pararaphs[i+1]["cues"] if i<len(pararaphs)-1 else pararaphs[-1]["cues"] #索引问题
        # Iterate over the cues
        for j, cue in enumerate(cues):
            # Get the start time, end time and text of the cue
            start_time = cue["time"]
            text = cue["text"]
            end_time = cues[j+1]["time"] if j < len(cues)-1 else cues_next[0]["time"]
            #start_time_obj = datetime.fromtimestamp(start_time/1000.0)
            #end_time_obj = datetime.fromtimestamp(end_time/1000.0)
            #subs.append(SubRipItem(cnt, start_time_obj.time(), end_time_obj.time(),text))
            subs.append(SubRipItem(cnt, start_time, end_time,text))

            cnt = cnt+1

    subs.save(s_utf8, encoding='utf-8')
    with open('output.txt','w', encoding='utf-8') as f:
        f.write(resp.text)
        f.close()



def download_progress(blocknum, blocksize, totalsize):
    readsofar = blocknum * blocksize
    if totalsize > 0:
        percent = readsofar * 1e2 / totalsize
        s = "\r%5.1f%% %*d / %d" % (
            percent, len(str(totalsize)), readsofar, totalsize)
        logging.info(s)


#@retry(stop_max_attempt_number=3, wait_fixed=1000, retry_on_exception=lambda x: isinstance(x, urllib.error.URLError))
def download_inner(urls,id):
    for  (count, url_dict) in enumerate(urls):
        url = url_dict['url']
        path = url_dict['path']   
        tmp_path = path+'_tmp'               
        #urllib.request.urlretrieve(url, path, reporthook=download_progress)
        if type(url) == str:
            try:
                if os.path.exists(tmp_path): #判断下载文件是否存在
                    downloaded_size = os.path.getsize(tmp_path) #获取已经下载部分的文件大小
                else:
                    downloaded_size = 0
                file_size = int(urlopen(url).info().get('Content-Length', -1)) #获取下载文件大小
                headers = {"Range": "bytes=%s-%s" % (downloaded_size, file_size)}
                response = requests.get(url,headers=headers, stream=True)
                t = tqdm(total=file_size, unit='B', initial=downloaded_size,unit_scale=True, desc=tmp_path)
                t.bar_format= tmp_path[-30:] + " : {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}{postfix}]"
                with open(tmp_path, 'ab') as fp:
                    for data in response.iter_content(1024):
                        fp.write(data)
                        t.update(1024)
                        if file_size != 0 and downloaded_size >= file_size:
                            break
                downloaded_size = os.path.getsize(tmp_path)
                if downloaded_size >= file_size:
                    os.rename(tmp_path, path)
                    logging.info("File downloaded and renamed successfully:%s",path)
                t.close()
            except BaseException  as e:
                logging.info('==============download error : %s, %s',e, url)
            


def download_in_parallel(concurreny_count,df,root_folder):
    ids = df['talk__id']
    
    urllist = []
    for index, row in df.iterrows():
        if not row['talk__id']:
            continue
        id = int(row['talk__id'])
        valid_file_name = re.sub(r'[^\w\s]|\t|\n|标题', '', row['talk__name'])
        dst_dir = root_folder+'/'+str(id)+'_'+valid_file_name.rstrip()
        slug = row['slug'].rstrip()
        if not os.path.exists(dst_dir):
            os.mkdir(dst_dir)
        video_file = dst_dir+'/'+slug+'.mp4'
        if not os.path.exists(video_file) and row['url__video']!='':
            d = dict()
            d['url'] = row['url__video']
            d['path'] = video_file
            urllist.append(d)
        mp3_file = dst_dir+'/'+slug+'.mp3'
        if not os.path.exists(mp3_file) and row['url__audio']!='':
            d = dict()
            d['url'] = row['url__audio']
            d['path'] = mp3_file 
            urllist.append(d)         
        h264_file = dst_dir+'/'+slug+'_h264.mp4'
        if not os.path.exists(h264_file) and row['url__h264']!='' and row['url__video'] =='': 
            d = dict()
            d['url'] = row['url__h264']
            d['path'] = h264_file
            urllist.append(d)
        jpg_file = dst_dir+'/'+slug+'.jpg'
        if not os.path.exists(jpg_file) and row['url__photo__talk']!='': 
            d = dict()
            d['url'] = row['url__photo__talk']
            d['path'] = jpg_file
            urllist.append(d)
        transcript_zh_file = dst_dir+'/'+slug+'_zh.srt'
        if not os.path.exists(transcript_zh_file) and row['transcript_zh-cn']!='': 
            transcript2src(transcript_zh_file,row['transcript_zh-cn'])
        transcript_en_file = dst_dir+'/'+slug+'_en.srt'
        if not os.path.exists(transcript_en_file) and row['transcript_en']!='': 
            transcript2src(transcript_en_file,row['transcript_en'])
    if len(urllist) > 0 :
        Processes = []
        urls_  =  [urllist[(i* (len(urllist)//concurreny_count)):((i+1)* (len(urllist)//concurreny_count))]    for i in range(concurreny_count)]

        leftovers  =  urllist[(concurreny_count * (len(urllist)//concurreny_count))  :  len(urllist)]
        for i in range(len(leftovers)):    
            urls_[i] += [leftovers[i]]


        for  (id_,urls__)  in enumerate(urls_):
            p = Process(target=download_inner, args=(urls__,id_))
            Processes.append(p)
            p.start()


        # block until all the threads finish (i.e. block until all function_x calls finish)
        for t in Processes:    
            t.join()

    