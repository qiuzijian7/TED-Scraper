import json
import datetime
from pysrt import SubRipItem, SubRipFile
# Load the TED transcript in JSON format
with open('ted_transcript.txt', 'r',encoding = 'utf-8') as f:
    transcript = json.load(f)



subs = SubRipFile()
cnt = 1
for paragraph in transcript["data"]["translation"]["paragraphs"]:
    # Get the cues list
    cues = paragraph["cues"]
    # Iterate over the cues
    for i, cue in enumerate(cues):
        # Get the start time, end time and text of the cue
        start_time = cue["time"]
        text = cue["text"]
        end_time = cues[i+1]["time"] if i < len(cues)-1 else cues[-1]["time"]
        start_time_obj = datetime.datetime.fromtimestamp(start_time/1000.0)
        end_time_obj = datetime.datetime.fromtimestamp(end_time/1000.0)
        subs.append(SubRipItem(cnt, start_time_obj.time(), end_time_obj.time(),text))
        cnt = cnt+1


# create the srt file
#srt_file = pysrt.compose(subs)
subs.save('output.srt', encoding='utf-8')