from bili_uper_status_aio import uper_status_update_many
from bili_uper_videos_aio import uper_videos_save_many
from bili_video_status_aio import video_status_update
from bili_video_danmus_aio2 import video_danmus_save

update_list = [7349]

uper_status_update_many(update_list)

uper_videos_save_many(update_list)

for uper in update_list:
    video_status_update(uper)

for uper in update_list:
    video_danmus_save(uper)
