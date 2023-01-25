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
    resp = requests.get(transcriptUrl, headers=headers)
    #logging.info(resp.content.decode())
    if 'errors' in resp.text:
        logging.info('error transcript: %s',transcriptUrl)
        return
    src_str =''
    try:
        data = json.loads(resp.text)
        if not data['data']['translation']:
            return
        src_str = data['data']['translation']['paragraphs']
    except json.decoder.JSONDecodeError as e:
        logging.info("An error occurred while parsing the JSON transcript:%s,%s", e,transcriptUrl)
        return
            
    # create an empty list to hold the srt lines
    srt_lines = []

    # iterate through the paragraphs and cues
    for i, paragraph in enumerate(src_str, start=1):
        for cue in paragraph["cues"]:
            # format the srt line
            start_time = cue["time"] / 1000
            text = cue["text"]
            srt_line = f"{i}\n{start_time:02}:{start_time:02} --> {start_time:02}:{start_time:02}\n{text}\n"
            # add the srt line to the list
            srt_lines.append(srt_line)

    # basename = os.path.basename(s_utf8)
    # # write the srt lines to a file
    # if len(basename)>250:
    #    basename = basename[-250:]
    # dirname = os.path.dirname(s_utf8)
    # s_utf8 = dirname+'/'+basename
    with open(s_utf8, "w", encoding='utf-8') as srt_file:
        srt_file.writelines(srt_lines)



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
        #urllib.request.urlretrieve(url, path, reporthook=download_progress)
        if not url:
            try:
                # with urllib.request.urlopen(url) as url, open(path, 'wb') as fp:
                #     meta = url.info()
                #     file_size = int(meta.get("Content-Length"))
                #     t = tqdm(total=file_size, unit='B', unit_scale=True, desc=path)
                #     t.bar_format= path[-30:] + " : {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}, {rate_fmt}{postfix}]"
                #     while True:
                #         buffer = url.read(8192)
                #         if not buffer:
                #             break
                #         fp.write(buffer)
                #         t.update(len(buffer))
                response = requests.get(url, stream=True)
                file_size = int(response.headers.get("Content-Length", 0))
                t = tqdm(total=file_size, unit='B', unit_scale=True, desc=path)
                with open(path, 'wb') as fp:
                    for data in response.iter_content(32*1024):
                        fp.write(data)
                        t.update(len(data))
            except BaseException  as e:
                logging.info('==============download error : %s, %s',e, url)


def download_in_parallel(concurreny_count,df,root_folder):
    ids = df['talk__id']
    
    urllist = []
    for index, row in df.iterrows():
        if not row['talk__id']:
            continue
        id = int(row['talk__id'])
        valid_file_name = re.sub(r'[^\w\s]', '', row['talk__name'])
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
        h264_file = dst_dir+'/'+slug+'.mp4'
        if not os.path.exists(h264_file) and row['url__h264']!='' and (row['url__audio']=='' or row['url__video'] ==''): 
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

    