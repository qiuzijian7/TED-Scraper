

#from moviepy.editor import VideoFileClip, TextClip

from moviepy.editor import *
from moviepy.video.compositing.concatenate import concatenate_videoclips
import pysrt
from datetime import datetime,  timedelta
from moviepy.config import change_settings
#change_settings({"TEXTCLIP_FONT": "C:\\Windows\\Fonts\\Arial.ttf"})

# Open the video file
video = VideoFileClip("video.mp4")
w, h = video.w, video.h
# Create a list of subtitles
subs = pysrt.open("video_zh.srt", encoding='utf-8')
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
video.write_videofile("output.mp4")

# # 读取视频
# clip = VideoFileClip("video.mp4").subclip(0, 20)
# clip = clip.volumex(0.8)
# # 文字视频
# text_clip: TextClip = TextClip("暂时无法显示中文，只能是English", fontsize=70, color='black')
# text_clip = text_clip.set_position("center").set_duration(20)

# # 合成视频
# composite_video_clip = CompositeVideoClip([clip, text_clip])

# # 导出视频
# composite_video_clip.write_videofile("output.mp4")

print('success')