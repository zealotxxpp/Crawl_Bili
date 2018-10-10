from bili_uper_status_aio import uper_status_update_many
from bili_uper_videos_aio import uper_videos_save_many
from bili_video_status_aio import video_status_update
from bili_video_todo_aio import update_todotimestamplist
from bili_video_danmus_aio3 import video_danmus_save

update_list = [7349]

cookies = {}

cookie_raw = input('cookies?')
for line in cookie_raw.split(';'):   #按照字符：进行划分读取
    #其设置为1就会把字符串拆分成2份
    name,value=line.strip().split('=',1)
    cookies[name]=value  #为字典cookies添加内容

uper_status_update_many(update_list)

uper_videos_save_many(update_list)

for uper in update_list:
    video_status_update(uper)

#for uper in update_list:
#    update_todotimestamplist(uper, cookies)

for uper in update_list:
    video_danmus_save(uper, cookies)
