
import requests, bs4, sys, os, time, json, re, random, ast
from multiprocessing import Process, Manager
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
from datetime import datetime
from requests.structures import CaseInsensitiveDict


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

def download(urls, id_, csv_list):
    print('download start:')
    for count, url in enumerate(urls):
        print(url)
        def get_transcript(url):

            url = "https://www.ted.com/graphql?operationName=Transcript&variables=%7B%22id%22%3A%22alexis_nikole_nelson_a_flavorful_field_guide_to_foraging%22%2C%22language%22%3A%22en%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%2218f8e983b84c734317ae9388c83a13bc98702921b141c2124b3ce4aeb6c48ef6%22%7D%7D"

            headers = CaseInsensitiveDict()
            headers["User-Agent"] = UserAgent
            headers["Accept"] = "*/*"
            headers["Accept-Language"] = "en-US,en;q=0.5"
            headers["Accept-Encoding"] = "gzip, deflate, br"
            headers["Referer"] = "https://www.ted.com/talks/alexis_nikole_nelson_a_flavorful_field_guide_to_foraging/transcript"
            headers["content-type"] = "application/json"
            headers["client-id"] = "Zenith production"
            headers["x-operation-name"] = "Transcript"
            resp = requests.get(url, headers=headers)
            print(resp.content)

            
            transcript = ""
            transcript_res = requests.get(url, headers = {'User-Agent': UserAgent})
                                                                        
            soup = BeautifulSoup(transcript_res.text)
            e = soup.select('span[class="inline cursor-pointer hover:bg-red-300 css-82uonn"]')

            for  e_  in e:
                classes = e_.get('class')
                text = e_.select('p')[0].text
                transcript += text.strip().replace('\t', '').replace('\n', ' ')
            
            if (transcript_res.status_code!=200) or (transcript_res.text=='') or (transcript==''):
                count_=0
                while  count_ < 3:    # Check 3 more times
                    time.sleep(random.randint(0,900)/1000)     # Randomly wait for 0-0.9 seconds.
                    transcript_res = requests.get(url, headers = {'User-Agent':UserAgent })

                    soup = BeautifulSoup(transcript_res.text)
                    e = soup.select('span[class="inline cursor-pointer hover:bg-red-300 css-82uonn"]')

                    for  e_  in e:
                        classes = e_.get('class')
                        text = e_.select('p')[0].text
                        transcript += text.strip().replace('\t', '').replace('\n', ' ')

                    count_ += 1
                    if (transcript_res.status_code==200) and (transcript_res.text!='') and (transcript!=''):    break

            return transcript

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
        d["comment_count"]  =  get_value(["pageProps","comments", "count"], metadata)


        d["duration"]  =  get_value(["pageProps","videoData","duration"], metadata)    # In seconds.


        language  =  get_value(["language"], metadata)
        url__transcript  =  url + "/transcript?language=" + language
        d["transcript"]  =  get_transcript(url__transcript)


        d["video_type_name"]  =  get_value(["pageProps", "videoData", "tpye","name"], metadata)    # One of:  TED Stage Talk, TEDx Talk, TED-Ed Original, TED Institute Talk, Best of Web, Original Content, TED Salon Talk (partner), Custom sponsored content
        d["event"]  =  get_value(["event"], playerDataMetadata)
        

        d["number_of__speakers"]  =  len(get_value(["pageProps", "videoData","speakers"], metadata) or "")

        # First speaker details.
        d["speaker__id"]  =  get_value(["pageProps", "videoData", "playerData", "id"], metadata)                        
        d["speaker__name"]  =  get_value(["pageProps", "videoData","presenterDisplayName"], metadata)   
        d["speaker__description"]  =  get_value(["pageProps", "videoData","speakers","Nodes", 0,"description"], metadata)
        d["speaker__who_he_is"]  =  get_value(["pageProps", "videoData","speakers","Nodes", 0, "whoTheyAre"], metadata)

        d["speaker__why_listen"]  =  html_to_text(get_value(["pageProps", "videoData","speakers","Nodes", 0, "whylisten"], metadata))

        d["speaker__what_others_say"]  =  get_value(["pageProps", "videoData","speakers","Nodes", 0, "whatotherssay"], metadata)#none
        d["speaker__is_published"]  =  get_value(["pageProps", "videoData","speakers","Nodes", 0, "is_published"], metadata)#none
        
        d["all_speakers_details"]  =  get_value(["speakers"], metadata)#none
        

        d["is_talk_featured"]  =  get_value(["pageProps", "videoData", "featured"], metadata)
        d["has_talk_citation"]  =  get_value(["pageProps", "videoData","speakers","Nodes", 0, "has_citations"], metadata)#none
        

        # Recorded and Published time.
        temp  =  get_value(["pageProps", "videoData", "recordedOn"], metadata)
        d["recording_date"]  =  temp  if temp==None  else temp[:10]
        
        t  =  get_value(["pageProps", "videoData", "publishedAt"], metadata)
        d["published_timestamp"]  =  datetime.utcfromtimestamp(int(t)).strftime('%Y-%m-%d %H:%M:%S')

        # Tags
        topicsLen = len(get_value(["pageProps", "videoData", "topics"], metadata))

        d["talks__tags"]  =  get_value(["targeting", "tag"], playerDataMetadata)
        d["number_of__tags"]  =  topicsLen or ""
        

        d["language"]  =  language
        d["native_language"]  =  get_value(["nativeLanguage"], playerDataMetadata)
        d["language_swap"]  =  get_value(["language_swap"], metadata)#none
                                
        d["is_subtitle_required"]  =  get_value(["isSubtitleRequired"], playerDataMetadata)
        

        # URLs.
        d["url__webpage"]  =  url #get_value(["url"], metadata)
        d["url__audio"]  =  get_value(["talks", 0, "downloads", "audioDownload"], metadata)#none
        d["url__video"]  =  get_value(["resources", "h264", "file"], playerDataMetadata)
        d["url__photo__talk"]  =  get_value(["talks", 0, "hero"], metadata)#none
        d["url__photo__speaker"]  =  get_value(["speakers", 0, "photo_url"], metadata)#none
        
        d["url__subtitled_videos"]  =  get_value(["talks", 0, "downloads", "subtitledDownloads"], metadata)#none
        d["number_of__subtitled_videos"]  =  len(get_value(["talks", 0, "downloads", "subtitledDownloads"], metadata) or "")#none
        
        d["talk__download_languages"]  =  get_value(["talks", 0, "downloads", "languages"], metadata)    # [?]#none
        d["number_of__talk__download_languages"]  =  len(get_value(["talks", 0, "downloads", "languages"], metadata) or "")    # [?]#none
        

        # More resources.
        d["talk__more_resources"]  =  get_value(["talks", 0, "more_resources"], metadata)#none
        d["number_of__talk__more_resources"]  =  len(get_value(["talks", 0, "more_resources"], metadata) or "")#none


        # Recommendations.
        d["talk__recommendations__blurb"]  =  get_value(["talks", 0, "recommendations", "blurb"], metadata)#none
        
        d["talk__recommendations"]  =  get_value(["talks", 0, "recommendations", "rec_lists"], metadata)#none
        d["number_of__talk__recommendations"]  =  len(get_value(["talks", 0, "recommendations", "rec_lists"], metadata) or "")#none


        # Related Talks.
        d["related_talks"]  =  get_value(["talks", 0, "related_talks"], metadata)#none
        d["number_of__related_talks"]  =  len(get_value(["talks", 0, "related_talks"], metadata) or "")#none


        # Durations.
        d["intro_duration"]  =  get_value(["talks", 0, "player_talks", 0, "introDuration"], metadata)    # [?]#none
        d["ad_duration"]  =  get_value(["talks", 0, "player_talks", 0, "adDuration"], metadata)    # [?]#none
        d["post_ad_duration"]  =  get_value(["talks", 0, "player_talks", 0, "postAdDuration"], metadata)    # [?]#none
        d["external__duration"]  =  get_value(["talks", 0, "player_talks", 0, "external", "duration"], metadata)    # [?]#none
        d["external__start_time"]  =  get_value(["talks", 0, "player_talks", 0, "external", "start_time"], metadata)    # [?]#none
        

        d["talks__player_talks__resources__h264__00__bitrate"]  =  get_value(["resources", "h264", "bitrate"], playerDataMetadata)    


        # [?] Take Action.
        d["talks__take_action"]  =  get_value(["talks", 0, "take_action"], metadata)#none
        d["number_of__talks__take_actions"]  =  len(get_value(["talks", 0, "take_action"], metadata) or "")#none


        csv_list.append(d)

if __name__ == '__main__':

    csv_list_ = []
    with  Manager()  as manager:
        csv_list = manager.list()    # SPECIAL variable - can be used only locally.
        Processess = []
        concurreny_count  =  1
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




