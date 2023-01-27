

#from moviepy.editor import VideoFileClip, TextClip
# 将h264或者mp4转成mp3
# 将英文srt合并到mp4
from moviepy.editor import *
from moviepy.video.compositing.concatenate import concatenate_videoclips
import pysrt
from datetime import datetime,  timedelta
from moviepy.config import change_settings
TedDataFolder = 'TED_DataBase'


if __name__ == '__main__':
    all = os.walk(TedDataFolder)
    for path,dir,filelist in all:
        for filename in filelist:
            if filename.endswith('.mp4') and not filename.endswith('_tmp.mp4'):
                print(filename)