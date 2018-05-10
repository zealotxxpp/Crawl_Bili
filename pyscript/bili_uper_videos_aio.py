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

def CT_uper_videos():

    global config

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = """create table uper_videos(uid varchar(20), mid varchar(20), aid varchar(20), aid_x varchar(20), comment int,
            typeid int, play int, pic varchar(100), subtitle varchar(50), description varchar(400), copyright varchar(20), title varchar(50),
            review int, author varchar(20), createdtime datetime, length varchar(20), video_review int, favorites int, hide_click bool,
            record_time datetime)engine=innodb charset utf8mb4"""
            cursor.execute(sql)

        connection.commit()
        print ('table up_baseinfo been created')
    finally:
        connection.close()

async def uper_videos_getonepage(uid, page, pagesize=100, order='pubdate'):

    url = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid=%s&pagesize=%d&page=%d&order=%s' % (uid, pagesize, page, order)
    headers = {'Referer': 'https://space.bilibili.com/%d'% uid}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url,  headers=headers) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

async def uper_videos_getall(uid):
    jstext_list = []

    text = await uper_videos_getonepage(uid, 1)
    jstext = js.loads(text,encoding='utf8')
    jstext_list.append(jstext)

    if jstext['status'] != True:
        print('#%s uper status False!' % uid)
        return

    count = jstext['data']['count']
    pages = jstext['data']['pages']

    if count == 0:
        print('#%s uper has no videos!' % uid)
        return

    print('#%s uper has %d videos!' % (uid, count))

    if pages == 1 :
        return jstext_list

    for i in range(2,pages+1):
        text = await uper_videos_getonepage(uid, i)
        jstext = js.loads(text,encoding='utf8')
        jstext_list.append(jstext)

    return jstext_list

def uper_videos_texttolist(jstext, uid):

    global uper_videos_list

    vlist = jstext['data']['vlist']

    record_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for vinfo in vlist:
        uid = uid
        mid = vinfo['mid']
        aid = vinfo['aid']
        aid_x = str(aid).zfill(12)
        comment = vinfo['comment']
        typeid = vinfo['typeid']
        try:
            play = int(vinfo['play'])
        except:
            play = None
        pic = vinfo['pic']
        subtitle = vinfo['subtitle']
        description = vinfo['description']
        copyright = vinfo['copyright']
        title = vinfo['title']
        review = vinfo['review']
        author = vinfo['author']
        created = datetime.datetime.fromtimestamp(max(vinfo['created'], 86400)).strftime('%Y-%m-%d %H:%M:%S')
        length = vinfo['length']
        video_review = vinfo['video_review']
        favorites = vinfo['favorites']
        hide_click = vinfo['hide_click']


        temp = [uid,mid,aid,aid_x,comment,typeid,play,pic,subtitle,description,copyright,title,review,
                author,created,length,video_review,favorites,hide_click,record_time]

        uper_videos_list.append(temp)

def uper_videos_insert(jstext_list, uid):
    global uper_videos_list

    uper_videos_list = []

    global config
    Insertnum_all = 0

    for jstext in jstext_list:

        uper_videos_texttolist(jstext, uid)


        connection = pymysql.connect(**config)

        try:
            with connection.cursor() as cursor:

                sql = """INSERT INTO uper_videos(uid,mid,aid,aid_x,comment,typeid,play,pic,subtitle,description,copyright,title,review,
                author,createdtime,length,video_review,favorites,hide_click,record_time)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

                try:
                    cursor.executemany(sql,uper_videos_list)
                    connection.commit()
                    print('INSET %d ROWS in Table uper_videos' % len(uper_videos_list))
                    Insertnum_all += len(uper_videos_list)
                except Exception as e:
                    print(e)
                    connection.rollback()
        finally:
            connection.close()

        uper_videos_list =[]

    print('#%s UPER INSET TOTALLY %d ROWS in Table uper_videos' % (uid, Insertnum_all))
    print('\r\n')


def uper_videos_save_many(uper_list):

    global uper_videos_list

    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    tasks = asyncio.gather(*[uper_videos_getall(i) for i in uper_list])
    try:
        jstext_lists = loop.run_until_complete(tasks)
    except NameError:
        print('too fast too fast')
        loop.close()
        time.sleep(60)
        uper_videos_save_many(uper_list)
        return
    print('\r\n')
    uperandjstext_list = zip(jstext_lists, uper_list)

    for jstext_list, uid in uperandjstext_list:
        if jstext_list == None:
            continue
        uper_videos_insert(jstext_list, uid)

uper_videos_list = []

if __name__ == '__main__':
    time1 = time.time()


#    st = 1000
#    ed = 1500

#    start = st * 10
#    end = start + 10

#    uper_list = [i for i in range(start, end)]

    uper_list = [7349, 7714, 13046]
    uper_videos_save_many(uper_list)

#    while st < ed - 1:
#        st = st + 1
#        start = st * 10
#        end = start + 10
#        uper_list = [i for i in range(start, end)]
#        uper_status_update_many(uper_list)

    time2 = time.time()
    print('videos info saving all done ' + 'total_time:' + str(time2 - time1))
