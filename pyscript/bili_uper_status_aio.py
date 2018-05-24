import datetime, pymysql
import json as js
import time
import asyncio, aiohttp

config = {
      'host':'localhost',
      'port':3306,
      'user':'root',
      'password':'ms020312',
      'db':'bili',
      'charset':'utf8mb4'
      }

def CT_uper_status():

    global config

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = """create table uper_status(uid varchar(20), video int, mid_x varchar(20), bangumi int, channel_master int, channel_guest int,
            favourite_master int, favourite_guest int, tag int, article int, playlist int, album int, following int, whisper int, black int,
            follower bigint, archive_view bigint, article_view bigint, vipType varchar(10), vipStatus varchar(10),
            elec_count int, elec_total_count int, notice varchar(200), notice_mtime datetime, record_time datetime)
            engine=innodb charset utf8mb4"""
            cursor.execute(sql)

        connection.commit()
        print ('table up_baseinfo been created')
    finally:
        connection.close()

async def uper_spacenavnum_get(uid):

    url = 'https://api.bilibili.com/x/space/navnum?mid=%d&jsonp=jsonp' % uid
    headers = {'Referer': 'https://space.bilibili.com/%d'% uid}
    #postload = {'mid': '%d' % uid, 'csrf': 'null'}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url,  headers=headers) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

async def uper_relationstat_get(uid):

    url = 'https://api.bilibili.com/x/relation/stat?vmid=%d&jsonp=jsonp' % uid
    headers = {'Referer': 'https://space.bilibili.com/%d'% uid}
    #postload = {'mid': '%d' % uid, 'csrf': 'null'}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url,  headers=headers) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

async def uper_spaceupstat_get(uid):

    url = 'https://api.bilibili.com/x/space/upstat?mid=%d&jsonp=jsonp' % uid
    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'}
    #postload = {'mid': '%d' % uid, 'csrf': 'null'}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url,  headers=headers) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

async def uper_vipstatus_get(uid):

    url = 'https://space.bilibili.com/ajax/member/getVipStatus?mid=%d' % uid
    #postload = {'mid': '%d' % uid, 'csrf': 'null'}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

async def uper_elec_get(uid):

    url = 'https://elec.bilibili.com/api/query.rank.do?mid=%d' % uid
    #postload = {'mid': '%d' % uid, 'csrf': 'null'}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

async def uper_notice_get(uid):

    url = 'https://space.bilibili.com/ajax/settings/getNotice?mid=%d' % uid
    #postload = {'mid': '%d' % uid, 'csrf': 'null'}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

async def uper_status_coll(uid):

    global uper_status_list

    text_spacenavnum = await uper_spacenavnum_get(uid)
    jstext_spacenavnum = js.loads(text_spacenavnum,encoding='utf8')
    data_spacenavnum = jstext_spacenavnum['data']

    text_relationstat = await uper_relationstat_get(uid)
    jstext_relationstat = js.loads(text_relationstat,encoding='utf8')
    data_relationstat = jstext_relationstat['data']

    text_spaceupstat = await uper_spaceupstat_get(uid)
    jstext_spaceupstat = js.loads(text_spaceupstat,encoding='utf8')
    data_spaceupstat = jstext_spaceupstat['data']

    text_vipstatus = await uper_vipstatus_get(uid)
    jstext_vipstatus = js.loads(text_vipstatus,encoding='utf8')
    data_vipstatus = jstext_vipstatus['data']

    text_elec = await uper_elec_get(uid)
    jstext_elec = js.loads(text_elec,encoding='utf8')


    text_notice = await uper_notice_get(uid)
    jstext_notice = js.loads(text_notice,encoding='utf8')


    uid = uid
    mid_x = str(uid).zfill(12)
    video = data_spacenavnum['video']
    bangumi = data_spacenavnum['bangumi']
    channel_master = data_spacenavnum['channel']['master']
    channel_guest = data_spacenavnum['channel']['guest']
    favourite_master = data_spacenavnum['channel']['master']
    favourite_guest = data_spacenavnum['channel']['guest']
    tag = data_spacenavnum['tag']
    article = data_spacenavnum['article']
    playlist = data_spacenavnum['playlist']
    album = data_spacenavnum['album']

    following = data_relationstat['following']
    whisper = data_relationstat['whisper']
    black = data_relationstat['black']
    follower = data_relationstat['follower']

    archive_view = data_spaceupstat['archive']['view']
    article_view = data_spaceupstat['article']['view']

    vipType = data_vipstatus['vipType']
    vipStatus = data_vipstatus['vipStatus']

    try:
        elec_count = jstext_elec['data']['count']
    except KeyError:
        elec_count = None
    try:
        elec_total_count = jstext_elec['data']['total_count']
    except KeyError:
        elec_total_count = None


    if jstext_notice['status'] == True:
        data_notice = jstext_notice['data']
        notice = data_notice['notice']
        try:
            notice_mtime = data_notice['modify_time']
        except KeyError:
            notice_mtime = None
    else:
        notice = None
        notice_mtime = None

    record_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    temp = [uid,mid_x,video,bangumi,channel_master,channel_guest,favourite_master,favourite_guest,tag,article,playlist,album,following,
            whisper,black,follower,archive_view,article_view,vipType,vipStatus,elec_count,elec_total_count,notice,notice_mtime,record_time]

    uper_status_list.append(temp)

def uper_status_coll2(uid):

    global uper_status_list

    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    tasks = asyncio.gather(uper_spacenavnum_get(uid),uper_relationstat_get(uid),uper_spaceupstat_get(uid),uper_vipstatus_get(uid),
                          uper_elec_get(uid),uper_notice_get(uid))

    try:
        text_list = loop.run_until_complete(tasks)
    except NameError:
        print('too fast too fast')
        loop.close()
        time.sleep(60)
        uper_status_coll2(uid)
        return



    text_spacenavnum = text_list[0]
    jstext_spacenavnum = js.loads(text_spacenavnum,encoding='utf8')
    data_spacenavnum = jstext_spacenavnum['data']

    text_relationstat = text_list[1]
    jstext_relationstat = js.loads(text_relationstat,encoding='utf8')
    data_relationstat = jstext_relationstat['data']

    text_spaceupstat = text_list[2]
    jstext_spaceupstat = js.loads(text_spaceupstat,encoding='utf8')
    data_spaceupstat = jstext_spaceupstat['data']

    text_vipstatus = text_list[3]
    jstext_vipstatus = js.loads(text_vipstatus,encoding='utf8')
    data_vipstatus = jstext_vipstatus['data']

    text_elec = text_list[4]
    jstext_elec = js.loads(text_elec,encoding='utf8')


    text_notice = text_list[5]
    jstext_notice = js.loads(text_notice,encoding='utf8')

    loop.close()


    uid = uid
    mid_x = str(uid).zfill(12)
    video = data_spacenavnum['video']
    bangumi = data_spacenavnum['bangumi']
    channel_master = data_spacenavnum['channel']['master']
    channel_guest = data_spacenavnum['channel']['guest']
    favourite_master = data_spacenavnum['channel']['master']
    favourite_guest = data_spacenavnum['channel']['guest']
    tag = data_spacenavnum['tag']
    article = data_spacenavnum['article']
    playlist = data_spacenavnum['playlist']
    album = data_spacenavnum['album']

    following = data_relationstat['following']
    whisper = data_relationstat['whisper']
    black = data_relationstat['black']
    follower = data_relationstat['follower']

    try:
        archive_view = data_spaceupstat['archive']['view']
    except TypeError:
        archive_view = None
    try:
        article_view = data_spaceupstat['article']['view']
    except TypeError:
        article_view = None

    vipType = data_vipstatus['vipType']
    vipStatus = data_vipstatus['vipStatus']

    try:
        elec_count = jstext_elec['data']['count']
    except KeyError:
        elec_count = None
    try:
        elec_total_count = jstext_elec['data']['total_count']
    except KeyError:
        elec_total_count = None


    if jstext_notice['status'] == True:
        data_notice = jstext_notice['data']
        notice = data_notice['notice']
        try:
            notice_mtime = data_notice['modify_time']
        except KeyError:
            notice_mtime = None
    else:
        notice = None
        notice_mtime = None

    record_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    temp = [uid,mid_x,video,bangumi,channel_master,channel_guest,favourite_master,favourite_guest,tag,article,playlist,album,following,
            whisper,black,follower,archive_view,article_view,vipType,vipStatus,elec_count,elec_total_count,notice,notice_mtime,record_time]

    uper_status_list.append(temp)

def uper_status_insert(uper_status_list):
    global config

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:

            sql = """INSERT INTO uper_status(uid,mid_x,video,bangumi,channel_master,channel_guest,favourite_master,favourite_guest,tag,article,
            playlist,album,following,whisper,black,follower,archive_view,article_view,vipType,vipStatus,elec_count,elec_total_count,notice,
            notice_mtime,record_time)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

            try:
                cursor.executemany(sql,uper_status_list)
                connection.commit()
                print('INSET %d ROWS in Table uper_status' % len(uper_status_list))
            except Exception as e:
                print(e)
                connection.rollback()
    finally:
        connection.close()

def uper_status_update_many(uper_list):
#    time1 = time.time()
    global uper_status_list
    uper_status_list = []
    for i in uper_list:
        uper_status_coll2(i)
        print('#%d UPER status been collected' % i)

    uper_status_insert(uper_status_list)
    print('%d upers status been updated' % len(uper_status_list))
    print('\r\n')
#    time2 = time.time()
    uper_status_list = []

uper_status_list = []

if __name__ == '__main__':
    time1 = time.time()


    st = 700
    ed = 800

    start = st * 10
    end = start + 10

    uper_list = [i for i in range(start, end)]
    uper_status_update_many(uper_list)

    while st < ed - 1:
        st = st + 1
        start = st * 10
        end = start + 10
        uper_list = [i for i in range(start, end)]
        uper_status_update_many(uper_list)

    time2 = time.time()
    print('status updated all done ' + 'total_time:' + str(time2 - time1))
