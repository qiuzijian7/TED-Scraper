

# 将标题和srt文件写入pdf
from moviepy.editor import *
from moviepy.video.compositing.concatenate import concatenate_videoclips
import pysrt
from datetime import datetime,  timedelta
from moviepy.config import change_settings
TedDataFolder = 'TED_DataBase'
def merge_srt_video(filename,en_srt_filename,bwriteaudio):
 # Open the video file
    video = VideoFileClip(filename)
    audio = video.audio
    w, h = video.w, video.h
    # Create a list of subtitles
    subs = pysrt.open(en_srt_filename, encoding='utf-8')
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
    name = filename.split()#保存mp4文件
    video.write_videofile("output.mp4")
    if bwriteaudio:
        audio.write_audiofile('output.mp3')

    print('success')

if __name__ == '__main__':
    all = os.walk(TedDataFolder)
    for path,dir,filelist in all:
        for filename in filelist:
            if filename.endswith('.mp4') and not filename.endswith('_tmp.mp4'):
                print(filename)
                merge_srt_video