

#from moviepy.editor import VideoFileClip, TextClip

from moviepy.editor import *
from moviepy.video.compositing.concatenate import concatenate_videoclips
import pysrt
from datetime import datetime,  timedelta
from moviepy.config import change_settings
import os

TedDataFolder = 'TED_DataBase'
def merge_srt_video(root,mp4filename,en_srt_filename,bwriteaudio):
 # Open the video file
    mp4file = root+"\\"+ mp4filename
    srtfile = root+"\\"+ en_srt_filename
    video = VideoFileClip(mp4file)
    audio = video.audio
    w, h = video.w, video.h
    # Create a list of subtitles
    subs = pysrt.open(srtfile, encoding='utf-8')
    subs_list = [(sub.start.to_time(), sub.end.to_time(), sub.text) for sub in subs] #.to_time()
    txt_clips = []
    # Add the subtitles to the video
    for sub in subs_list:
        start, end, text = sub
        start_second = timedelta(hours=start.hour, minutes=start.minute, seconds=start.second).total_seconds()
        end_second = timedelta(hours=end.hour, minutes=end.minute, seconds=end.second).total_seconds()

        txt_clip = TextClip(text, fontsize=80, font='SimHei',stroke_color="black",stroke_width=2.1, size=(w - 20, 200),align='center', color='white').set_duration(end_second-start_second).set_pos("bottom").set_start(start_second)
    
        txt_clips.append(txt_clip)


    video = CompositeVideoClip([video, *txt_clips])
    name = mp4filename.split(".mp4")[0]#保存mp4文件
    video.write_videofile(root+"//"+name+"_en_output.mp4")
    if bwriteaudio:
        audio.write_audiofile(root+"//"+name+"_output.mp3")
    print('success')

def has_mp3file(filelist):
    for filename in filelist:
        if filename.endswith('.mp3'):
            return True
    return False
def get_ensrt(filelist):
    for filename in filelist:
        if filename.endswith('_en.srt'):
            return filename
    return None
def get_mp4(filelist):
    for filename in filelist:
        if filename.endswith('.mp4') and not filename.endswith('_tmp.mp4'):
            return filename
    return None
def has_outputfile(filelist):
    for filename in filelist:
        if filename.endswith('output.mp4') :
            return True
    return False

if __name__ == '__main__':
    all = os.walk(TedDataFolder)
    mergelist= []
    convertlist=[]
    for path,dir,filelist in all:
        if len(filelist) > 0 and not has_outputfile(filelist):
            bwriteaudio =  not has_mp3file(filelist)
            ensrtfilename = get_ensrt(filelist)
            mp4filename = get_mp4(filelist)
            if ensrtfilename and mp4filename:
                merge_srt_video(path,mp4filename,ensrtfilename,bwriteaudio)
