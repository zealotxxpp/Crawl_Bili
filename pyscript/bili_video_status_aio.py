import datetime, pymysql
import json as js
import time
import asyncio, aiohttp
from pandas import Series, DataFrame
import pandas as pd

config = {
      'host':'localhost',
      'port':3306,
      'user':'root',
      'password':'ms020312',
      'db':'bili',
      'charset':'utf8mb4'
      }

def CT_video_status():

    global config

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = """create table video_status(uid varchar(20), aid varchar(20), aid_x varchar(20), view bigint, danmaku int, reply int,
            favorite int, coin int, share int, now_rank int, his_rank int, no_reprint int, copyright int, av_count int, count int,
            total_count int, page int,record_time datetime)
            engine=innodb charset utf8mb4"""
            cursor.execute(sql)

        connection.commit()
        print ('table video_status been created')
    finally:
        connection.close()


def CT_video_pages():

    global config

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = """create table video_pages(uid varchar(20), aid varchar(20), cid varchar(20), cid_x varchar(20), page int,
            from_source varchar(50), part varchar(50), duration int, vid varchar(20), weblink varchar(100), record_time datetime)engine=innodb charset utf8mb4"""
            cursor.execute(sql)

        connection.commit()
        print ('table video_pages been created')
    finally:
        connection.close()

async def video_stat_get(aid):

    url = 'https://api.bilibili.com/x/web-interface/archive/stat?aid=%s' % aid

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

async def video_elec_get(aid, mid):

    url = 'https://api.bilibili.com/x/web-interface/elec/show?jsonp=jsonp&aid=%s&mid=%s' % (aid, mid)

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

async def video_pagelist_get(aid):

    url = 'https://api.bilibili.com/x/player/pagelist?aid=%s&jsonp=jsonp' % aid

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

async def video_status_coll(aid, mid, newvideolist):

    global video_status_list
    global video_pages_list

    text_stat = await video_stat_get(aid)
    jstext_stat = js.loads(text_stat,encoding='utf8')
    data_stat = jstext_stat['data']

    text_elec = await video_elec_get(aid, mid)
    jstext_elec = js.loads(text_elec,encoding='utf8')
    data_elec = jstext_elec['data']

    text_pagelist = await video_pagelist_get(aid)
    jstext_pagelist = js.loads(text_pagelist,encoding='utf8')
    data_pagelist = jstext_pagelist['data']



    mid = mid
    aid = data_stat['aid']
    aid_x = str(aid).zfill(12)
    try:
        view = int(data_stat['view'])
    except ValueError:
        view = None
    danmaku = data_stat['danmaku']
    reply = data_stat['reply']
    favorite = data_stat['favorite']
    coin = data_stat['coin']
    share = data_stat['share']
    now_rank = data_stat['now_rank']
    his_rank = data_stat['his_rank']
    no_reprint = data_stat['no_reprint']
    copyright = data_stat['copyright']
    try:
        av_count = data_elec['av_count']
    except TypeError:
        av_count = None
    try:
        count = data_elec['count']
    except TypeError:
        count = None
    try:
        total_count = data_elec['total_count']
    except TypeError:
        total_count = None
    page = len(data_pagelist)



    record_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    temp = [mid,aid,aid_x,view,danmaku,reply,favorite,coin,share,now_rank,his_rank,no_reprint,copyright,av_count,count,
            total_count,page,record_time]

    video_status_list.append(temp)

    print('video aid:%s mid:%s been collected' % (aid ,mid))


    if str(aid) in newvideolist:
        for onepage in data_pagelist:
            uid = mid
            cid = onepage['cid']
            cid_x = str(cid).zfill(12)
            page = onepage['page']
            from_source = onepage['from']
            part = onepage['part']
            duration = onepage['duration']
            vid = onepage['vid']
            weblink = onepage['weblink']

            temp = [uid,aid,cid,cid_x,page,from_source,part,duration,vid,weblink,record_time]

            video_pages_list.append(temp)
            print('page aid:%s mid:%s cid:%s been collected' % (aid ,mid, cid))

def video_status_insert(video_status_list):

    global config

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:

            sql = """INSERT INTO video_status(uid,aid,aid_x,view,danmaku,reply,favorite,coin,share,now_rank,his_rank,no_reprint,copyright,av_count,count,
            total_count,page,record_time)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

            try:
                cursor.executemany(sql,video_status_list)
                connection.commit()
                print('INSET %d ROWS in Table video_status' % len(video_status_list))
            except Exception as e:
                print(e)
                connection.rollback()
                raise Exception
    finally:
        connection.close()

def video_pages_insert(video_pages_list):

    global config

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:

            sql = """INSERT INTO video_pages(uid,aid,cid,cid_x,page,from_source,part,duration,vid,weblink,record_time)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

            try:
                cursor.executemany(sql,video_pages_list)
                connection.commit()
                print('INSET %d ROWS in Table video_pages' % len(video_pages_list))
            except Exception as e:
                print(e)
                connection.rollback()
                raise Exception
    finally:
        connection.close()

def videolist_get(uid):

    global config

    connection = pymysql.connect(**config)

    cur = connection.cursor(pymysql.cursors.DictCursor)

    sql = "SELECT distinct aid FROM uper_videos WHERE mid = %d"
    cur.execute(sql % uid)

    data = cur.fetchall()
    videolist = []
    if len(data) == 0:
        print('uid: %s has no video in Database' % uid)
        return videolist

    DF = DataFrame(data)
    for i in range(len(DF)):
        videolist.append(DF.iloc[i].values[0])

    print('uid: %s has %s video in Database' % (uid, len(videolist)))
    return videolist

def newvideolist_get(uid):

    global config

    connection = pymysql.connect(**config)

    cur = connection.cursor(pymysql.cursors.DictCursor)

    sql = "SELECT distinct aid FROM uper_videos b WHERE mid = %d and b.aid not in (SELECT distinct aid FROM video_pages)"
    cur.execute(sql % uid)

    data = cur.fetchall()
    newvideolist = []
    if len(data) == 0:
        print('uid: %s has no new video in Database' % uid)
        return newvideolist

    DF = DataFrame(data)
    for i in range(len(DF)):
        newvideolist.append(DF.iloc[i].values[0])

    print('uid: %s has %s new video in Database' % (uid, len(newvideolist)))
    return newvideolist

def video_status_update(uid):
    global video_status_list
    global video_pages_list

    videolist = videolist_get(uid)
    newvideolist = newvideolist_get(uid)
    print('\r\n')
    listnum = len(videolist)
    roundnum = (listnum // 10) + 1

    st = 0
    InsertNum1 = 0
    InsertNum2 = 0

    while st < roundnum:
        start = st * 10
        end = start + 10

        video_status_list = []
        video_pages_list = []
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        tasks = asyncio.gather(*[video_status_coll(aid, uid, newvideolist) for aid in videolist[start:end]])
        st = st + 1
        try:
            loop.run_until_complete(tasks)
        except NameError:
            print('too fast too fast')
            loop.close()
            time.sleep(60)
            st = st - 1
            continue
        loop.close()


        try:
            video_status_insert(video_status_list)
            InsertNum1 = InsertNum1 + len(video_status_list)
        except Exception as e:
            print(e)
            return Error

        if len(video_pages_list) == 0:
            continue

        try:
            video_pages_insert(video_pages_list)
            InsertNum2 = InsertNum2 + len(video_pages_list)
        except Exception:
            return



    print ('Totally #%s upper insert %d ROWS in Table video_status' % (uid, InsertNum1))
    print ('Totally #%s upper insert %d ROWS in Table video_pages' % (uid, InsertNum2))

video_status_list = []
video_pages_list = []

if __name__ == '__main__':
    time1 = time.time()

    uper_list = [7349]
    for uper in uper_list:
        video_status_update(uper)

    time2 = time.time()
    print('status updated all done ' + 'total_time:' + str(time2 - time1))
