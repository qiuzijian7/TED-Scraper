
import requests, bs4, sys, os, time, json, re, random, ast
from multiprocessing import Process, Manager
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime
from requests.structures import CaseInsensitiveDict
from selenium import webdriver
import openpyxl

'''  IGNORING WARNINGS  '''
import warnings
warnings.filterwarnings('ignore')


'''  NOTEBOOK DISPLAY SETTINGS  '''
pd.options.display.max_rows = 10000    # None
pd.options.display.max_columns = None

pd.set_option('display.max_colwidth', -1)
pd.set_option('display.expand_frame_repr', False)
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:95% !important; }</style>"))

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
driver = webdriver.Chrome(options=opt)  #创建浏览器对象

def download(urls, id_, csv_list):
    print('download start:')
    for count, url in enumerate(urls):
        print(url)
        def get_transcript(name,url,language):
           # transcriptUrl = 'https://www.ted.com/graphql?operationName=Transcript&variables={"id":"alexis_nikole_nelson_a_flavorful_field_guide_to_foraging","language":"zh-cn"}&extensions={"persistedQuery":{"version":1,"sha256Hash":"906b90e820733c27cab3bb5de1cb4578657af4610c346b235b4ece9e89dc88bd"}}'
            url = url.strip().replace('\t', '').replace('\n', ' ')
            transcriptUrl = 'https://www.ted.com/graphql?operationName=Transcript&variables={"id":"' + name + '","language":"'+language+'"}&extensions={"persistedQuery":{"version":1,"sha256Hash":"906b90e820733c27cab3bb5de1cb4578657af4610c346b235b4ece9e89dc88bd"}}'

            headers = CaseInsensitiveDict()
            headers["User-Agent"] = UserAgent
            headers["Accept"] = "*/*"
            headers["Accept-Language"] = "en-US,en;q=0.5"
            headers["Accept-Encoding"] = "gzip, deflate, br"
            headers["Referer"] = url+'/transcript'
            headers["content-type"] = "application/json"
            headers["client-id"] = "Zenith production"
            headers["x-operation-name"] = "Transcript"
            resp = requests.get(transcriptUrl, headers=headers)
            #print(resp.content.decode())
            return resp.text
        def get_video_audio_url(url):          
            driver.get(url) #打开网页
            # driver.maximize_window()   #最大化窗口
            time.sleep(1)     #加载等待
            try:
                btn = driver.find_element("xpath",'//div/Button[@class="rounded-sm text-sm font-medium"]/div/div[contains(text(),"Share")]').click()
            except:
                print("Share button not found!")
            try:
                element_video = driver.find_element("xpath",'//a[starts-with(@href,"https://download.ted.com/products/")]')
                video_url = element_video.get_attribute('href')
            except:
                print("element_video not found!")
                video_url = ''
            try:
                element_audio = driver.find_element("xpath",'//a[starts-with(@href,"https://download.ted.com/talks/")]')
                audio_url = element_audio.get_attribute('href')
            except:
                print("element_video not found!")
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
                    print("Connection refused by the server..")
                    print("Let me sleep for 5 seconds")
                    print("ZZzzzz...")
                    time.sleep(5)
                    print("Was a nice sleep, now let me continue...")
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
            print('url failed:', url);
            continue;
        else:  
            print('url success:', url);        
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

        playerData = get_value(["pageProps","videoData","playerData"], metadata)
        playerDataMetadata = json.loads(playerData)
        d = dict()

        # ALL THESE SPEAKER ATTRIBUTES CORRESPOND TO THE 1ST SPEAKER.
        d["talk__id"]  =  get_value(["pageProps","videoData","id"], metadata)
        d["talk__name"]  =  get_value(["pageProps","videoData","title"], metadata)
        d["talk__description"]  =  get_value(["pageProps","videoData","description"], metadata)


        d["view_count"]  =  get_value(["pageProps","videoData","viewedCount"], metadata)
        #d["comment_count"]  =  get_value(["pageProps","comments", "count"], metadata)


        d["duration"]  =  get_value(["pageProps","videoData","duration"], metadata)    # In seconds.

        slug = get_value(["pageProps","videoData","slug"], metadata)
        language  =  get_value(["language"], metadata)
        #url__transcript  =  url + "/transcript?language=" + language
        d["transcript_zh-cn"]  =  get_transcript(slug,url,'zh-cn')
        d["transcript_en"]  =  get_transcript(slug,url,'en')


        d["video_type_name"]  =  get_value(["pageProps", "videoData", "tpye","name"], metadata)    # One of:  TED Stage Talk, TEDx Talk, TED-Ed Original, TED Institute Talk, Best of Web, Original Content, TED Salon Talk (partner), Custom sponsored content
        d["event"]  =  get_value(["event"], playerDataMetadata)
        

        d["number_of__speakers"]  =  len(get_value(["pageProps", "videoData","speakers"], metadata) or "")

        # First speaker details.
        d["speaker__id"]  =  get_value(["pageProps", "videoData", "playerData", "id"], metadata)                        
        d["speaker__name"]  =  get_value(["pageProps", "videoData","presenterDisplayName"], metadata)   
        d["speaker__description"]  =  get_value(["pageProps", "videoData","speakers","Nodes", 0,"description"], metadata)
        d["speaker__who_he_is"]  =  get_value(["pageProps", "videoData","speakers","Nodes", 0, "whoTheyAre"], metadata)

        d["speaker__why_listen"]  =  html_to_text(get_value(["pageProps", "videoData","speakers","Nodes", 0, "whylisten"], metadata))

        #d["speaker__what_others_say"]  =  get_value(["pageProps", "videoData","speakers","Nodes", 0, "whatotherssay"], metadata)#none
        #d["speaker__is_published"]  =  get_value(["pageProps", "videoData","speakers","Nodes", 0, "is_published"], metadata)#none
        
        #d["all_speakers_details"]  =  get_value(["speakers"], metadata)#none
        

        d["is_talk_featured"]  =  get_value(["pageProps", "videoData", "featured"], metadata)
        d["has_talk_citation"]  =  get_value(["pageProps", "videoData","speakers","Nodes", 0, "has_citations"], metadata)
        

        # Recorded and Published time.
        temp  =  get_value(["pageProps", "videoData", "recordedOn"], metadata)
        d["recording_date"]  =  temp  if temp==None  else temp[:10]
        
        d["published_timestamp"]  =  get_value(["pageProps", "videoData", "publishedAt"], metadata)

        # Tags
        topicsLen = len(get_value(["pageProps", "videoData", "topics","nodes"], metadata))

        d["talks__tags"]  =  get_value(["targeting", "tag"], playerDataMetadata)
        d["number_of__tags"]  =  topicsLen or ""
        

        d["language"]  =  language
        d["native_language"]  =  get_value(["nativeLanguage"], playerDataMetadata)
        #d["language_swap"]  =  get_value(["language_swap"], metadata)#none
                                
        d["is_subtitle_required"]  =  get_value(["isSubtitleRequired"], playerDataMetadata)
        

        # URLs.
        d["url__webpage"]  =  url #get_value(["url"], metadata)
        url__video, url__audio =get_video_audio_url(url)
        d["url__audio"]  =  url__audio
        d["url__video"]  =  url__video
        d["url__h264"]  =  get_value(["resources", "h264",0,"file"], playerDataMetadata)
        d["url__bitrate"]  =  get_value(["resources", "h264",0, "bitrate"], playerDataMetadata)
        d["url__photo__talk"]  =  get_value(["pageProps", "videoData","primaryImageSet", 0, "url"], metadata)#none
        #d["url__photo__speaker"]  =  get_value(["speakers", 0, "photo_url"], metadata)#none
        
        #d["url__subtitled_videos"]  =  get_value(["talks", 0, "downloads", "subtitledDownloads"], metadata)#none
        #d["number_of__subtitled_videos"]  =  len(get_value(["talks", 0, "downloads", "subtitledDownloads"], metadata) or "")#none
        
        #d["talk__download_languages"]  =  get_value(["talks", 0, "downloads", "languages"], metadata)    # [?]#none
        #d["number_of__talk__download_languages"]  =  len(get_value(["talks", 0, "downloads", "languages"], metadata) or "")    # [?]#none
        

        # More resources.
        #d["talk__more_resources"]  =  get_value(["talks", 0, "more_resources"], metadata)#none
        #d["number_of__talk__more_resources"]  =  len(get_value(["talks", 0, "more_resources"], metadata) or "")#none


        # Recommendations.
        #d["talk__recommendations__blurb"]  =  get_value(["talks", 0, "recommendations", "blurb"], metadata)#none
        
        #d["talk__recommendations"]  =  get_value(["talks", 0, "recommendations", "rec_lists"], metadata)#none
        #d["number_of__talk__recommendations"]  =  len(get_value(["talks", 0, "recommendations", "rec_lists"], metadata) or "")#none


        # Related Talks.
        #d["related_talks"]  =  get_value(["talks", 0, "related_talks"], metadata)#none
        #d["number_of__related_talks"]  =  len(get_value(["talks", 0, "related_talks"], metadata) or "")#none


        # Durations.
        #d["intro_duration"]  =  get_value(["talks", 0, "player_talks", 0, "introDuration"], metadata)    # [?]#none
        #d["ad_duration"]  =  get_value(["talks", 0, "player_talks", 0, "adDuration"], metadata)    # [?]#none
        #d["post_ad_duration"]  =  get_value(["talks", 0, "player_talks", 0, "postAdDuration"], metadata)    # [?]#none
        #d["external__duration"]  =  get_value(["talks", 0, "player_talks", 0, "external", "duration"], metadata)    # [?]#none
        #d["external__start_time"]  =  get_value(["talks", 0, "player_talks", 0, "external", "start_time"], metadata)    # [?]#none
        

        #d["talks__player_talks__resources__h264__00__bitrate"]  =  get_value(["resources", "h264", "bitrate"], playerDataMetadata)    


        # [?] Take Action.
        #d["talks__take_action"]  =  get_value(["talks", 0, "take_action"], metadata)#none
        #d["number_of__talks__take_actions"]  =  len(get_value(["talks", 0, "take_action"], metadata) or "")#none


        csv_list.append(d)

if __name__ == '__main__':

    csv_list_ = []
    with  Manager()  as manager:
        csv_list = manager.list()    # SPECIAL variable - can be used only locally.
        Processess = []
        concurreny_count  =  100
        urls_  =  [urllines[(i* (len(urllines)//concurreny_count)):((i+1)* (len(urllines)//concurreny_count))]    for i in range(concurreny_count)]

        leftovers  =  urllines[(concurreny_count * (len(urllines)//concurreny_count))  :  len(urllines)]
        for i in range(len(leftovers)):    
            urls_[i] += [leftovers[i]]

        for  (id_,urls__)  in enumerate(urls_):
            time.sleep(random.randint(0,900)/1000)     # Randomly wait for 0-0.9 seconds.
            p = Process(target=download, args=(urls__,id_,csv_list))
            Processess.append(p)
            p.start()



        # block until all the threads finish (i.e. block until all **download** function calls finish)
        for t in Processess:    
            time.sleep(random.randint(0,900)/1000)     # Randomly wait for 0-0.9 seconds.
            t.join()

        csv_list_ = list(csv_list)


    print(csv_list_)
    # Creating DataFrame.
    df  =  pd.DataFrame(csv_list_)

    # Sort - most popular first.
    df = df.sort_values("view_count", ascending=False)

    # Saving.
    df.to_excel('TED_Talk.xlsx', encoding='utf-8', index=False)
    df.to_csv('TED_Talk.csv', index=False, encoding='utf-8')
    print(round(time.time()  -  time__))



