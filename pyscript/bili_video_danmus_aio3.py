import datetime, pymysql
import time
import asyncio, aiohttp
import numpy as np
from pandas import Series, DataFrame
import pandas as pd
from bs4 import BeautifulSoup as BS

config = {
          'host':'localhost',
          'port':3306,
          'user':'root',
          'password':'ms020312',
          'db':'bili',
          'charset':'utf8mb4'
          }

video_danmus_list = []
newdanmuxml_list = []
timestamps_done_list = []
cidrowID_DF = None
cookies = {}

def CT_video_danmus():

    global config

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = """create table video_danmus(uid varchar(20), aid varchar(20), cid varchar(20),source_timestamp varchar(20), danmu_reptime int,
            danmu_type varchar(20), danmu_size int, danmu_color varchar(20), danmu_creattime datetime, danmu_pole int, sender_ID varchar(20), rowID varchar(20),
            danmu_message varchar(300), record_time datetime)engine=innodb charset utf8mb4"""
            cursor.execute(sql)

        connection.commit()
    finally:
        connection.close()

def CT_video_timestamps_done():

    global config

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = """create table video_timestamps_done(uid varchar(20), aid varchar(20), cid varchar(20),source_timestamp varchar(20))engine=innodb charset utf8mb4"""
            cursor.execute(sql)

        connection.commit()
    finally:
        connection.close()

async def danmu_xml_get(url):
    global cookies
    async with aiohttp.ClientSession(cookies=cookies) as session:
        try:
            async with session.get(url=url) as resp:
                text = await resp.text(errors = "ignore")

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

        except asyncio.TimeoutError:
            return

def get_cidtimestamp_DF(uid):
    global config

    connection = pymysql.connect(**config)

    cur = connection.cursor(pymysql.cursors.DictCursor)

    sql = "SELECT cid, source_timestamp FROM video_timestamps_done WHERE uid = %s GROUP BY cid, source_timestamp"
    cur.execute(sql % uid)

    data = cur.fetchall()
    cur.close()
    connection.close()

    if len(data) == 0:
        DF = DataFrame()
        print('uid:%s has no cidtimestamp in Database video_timestamps_done' % uid)
        return DF

    DF = DataFrame(data)
    print('uid:%s has %s cidtimestamp in Database video_timestamps_done' % (uid,len(DF)))
    return DF

def get_cidtodotimestamp_DF(uid):
    global config

    connection = pymysql.connect(**config)

    cur = connection.cursor(pymysql.cursors.DictCursor)

    sql = "SELECT cid, todo_timestamp FROM video_timestamps_todo WHERE uid = %s GROUP BY cid, todo_timestamp"
    cur.execute(sql % uid)

    data = cur.fetchall()
    cur.close()
    connection.close()

    if len(data) == 0:
        DF = DataFrame()
        print('uid:%s has no cidtimestamp in Database video_timestamps_todo' % uid)
        return DF

    DF = DataFrame(data)
    print('uid:%s has %s cidtimestamp in Database video_timestamps_todo' % (uid,len(DF)))
    return DF

def get_cidrowID_DF(uid):
    global config

    connection = pymysql.connect(**config)

    cur = connection.cursor(pymysql.cursors.DictCursor)

    sql = "SELECT cid, rowID FROM video_danmus WHERE uid = %s"
    cur.execute(sql % uid)

    data = cur.fetchall()
    cur.close()
    connection.close()

    if len(data) == 0:
        DF = DataFrame()
        print('uid:%s has no cidrowID in Database' % uid)
        return DF

    DF = DataFrame(data)
    print('uid:%s has %s cidrowID in Database' % (uid,len(DF)))
    return DF

async def video_danmus_coll(uid, aid, cid, source_timestamp, danmu_xml_url):
    global video_danmus_list
    global cidrowID_DF
    global timestamps_done_list

    text = await danmu_xml_get(danmu_xml_url)

    if text == None:
        return
        
    soup = BS(text, 'lxml')
    all_d = soup.select('d')
    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        rowIDs = cidrowID_DF.rowID[cidrowID_DF.cid == str(cid)].unique()
    except AttributeError:
        rowIDs = []

    collnum = 0

    for d in all_d:
        danmu_list = d['p'].split(',')

        rowID = danmu_list[7]
        if str(rowID) in rowIDs:
            continue

        danmu_reptime = danmu_list[0]
        danmu_type = danmu_list[1]
        danmu_size = danmu_list[2]
        danmu_color = danmu_list[3]
        danmu_creattime  = datetime.datetime.fromtimestamp(int(danmu_list[4])).strftime('%Y-%m-%d %H:%M:%S')
        danmu_pole = danmu_list[5]
        sender_ID = danmu_list[6]
        rowID = danmu_list[7]
        danmu_message = d.get_text()
        record_time = now_str

        temp = [uid,aid,cid,source_timestamp,danmu_reptime,danmu_type,danmu_size,danmu_color,danmu_creattime,danmu_pole,sender_ID,
                rowID,danmu_message,record_time]

        video_danmus_list.append(temp)
        collnum = collnum + 1

    print('uid:%s aid:%s url:%s been collocted: %s danmu' % (uid, aid, danmu_xml_url, collnum))

    if source_timestamp == 'current':
        return
    temp2 = [uid, aid, cid, source_timestamp]
    timestamps_done_list.append(temp2)
    print('uid:%s aid:%s source_timestamp:%s url:%s been registered' % (uid, aid, source_timestamp, danmu_xml_url))

def video_danmus_insert(video_danmus_list, timestamps_done_list):

    global config

    connection = pymysql.connect(**config)
    try:
        with connection.cursor() as cursor:

            sql = """INSERT INTO video_danmus(uid,aid,cid,source_timestamp,danmu_reptime,danmu_type,danmu_size,danmu_color,danmu_creattime,danmu_pole,sender_ID,
                rowID,danmu_message,record_time)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

            try:
                cursor.executemany(sql,video_danmus_list)
                connection.commit()
                print('INSET %d ROWS in Table video_danmus' % len(video_danmus_list))
            except Exception as e:
                print(e)
                connection.rollback()
                raise Exception



            sql = """INSERT INTO video_timestamps_done(uid,aid,cid,source_timestamp)VALUES(%s,%s,%s,%s);"""

            try:
                cursor.executemany(sql,timestamps_done_list)
                connection.commit()
                print('INSET %d ROWS in Table video_timestamps_done' % len(timestamps_done_list))
            except Exception as e:
                print(e)
                connection.rollback()
                raise Exception


    finally:
        connection.close()

def get_cid_list(uid):
    global config

    connection = pymysql.connect(**config)

    cur = connection.cursor(pymysql.cursors.DictCursor)

    sql = "SELECT aid, cid FROM video_pages WHERE uid = %d order by cid_x desc"
    cur.execute(sql % uid)

    data = cur.fetchall()
    cidlist = []
    if len(data) == 0:
        print('uid: %s has no cid in Database' % uid)
        return cidlist

    DF = DataFrame(data)
    for i in range(len(DF)):
        cidlist.append([DF.iloc[i].values[0],DF.iloc[i].values[1]])

    print('uid: %s has %s cids in Database' % (uid, len(cidlist)))
    return cidlist

def create_newdanmuxml_list(uid):

    global newdanmuxml_list
    global config
    
    cidlist = get_cid_list(uid)
        
    for [aid, cid] in cidlist:
        source_timestamp = 'current'
        danmu_xml_url = 'http://comment.bilibili.com/%s.xml' % (cid)
        temp = [uid, aid, cid, source_timestamp, danmu_xml_url]
        newdanmuxml_list.append(temp)
    print('uid: %s has %s current danmuxml_url' % (uid, len(newdanmuxml_list)))
    
    cidtodotimestamp_DF = get_cidtodotimestamp_DF(uid)
    ciddonetimestamp_DF = get_cidtimestamp_DF(uid)
    
    for [aid, cid] in cidlist:
        collnum = 0
        url_temp = []
        timestamps_todo = cidtodotimestamp_DF.todo_timestamp[cidtodotimestamp_DF.cid == str(cid)].unique()
        timestamps_done = ciddonetimestamp_DF.source_timestamp[ciddonetimestamp_DF.cid == str(cid)].unique()
        for stamp in timestamps_todo:
            if stamp in timestamps_done:
                continue
            date = datetime.datetime.strftime(datetime.datetime.fromtimestamp(int(stamp)), '%Y-%m-%d')
            danmu_xml_url = 'https://api.bilibili.com/x/v2/dm/history?type=1&oid=%s&date=%s' % (cid, date)
            temp = [uid, aid, cid, stamp, danmu_xml_url]
            url_temp.append(temp)
            collnum = collnum + 1
        print('uid: %s aid: %s cid: %s has %s new rolldates to be collected' % (uid, aid, cid, collnum))
        newdanmuxml_list.extend(url_temp)


def video_danmus_save(uid, newcookies):
    global newdanmuxml_list
    global video_danmus_list
    global timestamps_done_list
    global cidrowID_DF
    global cookies

    cookies = newcookies

    newdanmuxml_list = []
    create_newdanmuxml_list(uid)
    
    print('uid: %s has %s newdanmuxml to be saved'% (uid, len(newdanmuxml_list)))
    print('\r\n')


    newdanmuxml_num = len(newdanmuxml_list)
    roundnum = (newdanmuxml_num // 50) + 1

    st = 0

    while st < roundnum:
        st_inter = st + 10
        st_inter = min(st_inter, roundnum)
        video_danmus_list = []
        timestamps_done_list = []
        cidrowID_DF = get_cidrowID_DF(uid)
        while st < st_inter:
            start = st * 50
            end = start + 50

            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
            tasks = asyncio.gather(*[video_danmus_coll(uid, aid, cid, source_timestamp, danmu_xml_url) for
                                     [uid, aid, cid, source_timestamp, danmu_xml_url] in newdanmuxml_list[start:end]])

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

        orilistnum = len(video_danmus_list)
        orilistnum2 = len(timestamps_done_list)

        if orilistnum == 0:
            video_danmus_list = []
        else:
            video_danmus_list_DF = DataFrame(video_danmus_list)
            video_danmus_list_DFunique = video_danmus_list_DF.drop_duplicates(video_danmus_list_DF[[2,11]])
            arr = np.array(video_danmus_list_DFunique)
            video_danmus_list = arr.tolist()
        inslistnum = len(video_danmus_list)
        delnum = orilistnum - inslistnum
        print('danmu orinum:%s insnum:%s delnum:%s' % (orilistnum, inslistnum, delnum))

        if orilistnum2 == 0:
            timestamps_done_list = []
        else:
            timestamps_done_list_DF = DataFrame(timestamps_done_list)
            timestamps_done_list_DFunique = timestamps_done_list_DF.drop_duplicates(timestamps_done_list_DF[[2,3]])
            arr = np.array(timestamps_done_list_DFunique)
            timestamps_done_list = arr.tolist()
        inslistnum2 = len(timestamps_done_list)
        delnum2 = orilistnum2 - inslistnum2
        print('timestamp orinum:%s insnum:%s delnum:%s' % (orilistnum2, inslistnum2, delnum2))

        video_danmus_insert(video_danmus_list, timestamps_done_list)
        print('\r\n')





if __name__ == '__main__':
    
    uid_list = [7349]
    
    newcookies = {}
    cookie_raw = input('cookies?')
    for line in cookie_raw.split(';'):   #按照字符：进行划分读取
        #其设置为1就会把字符串拆分成2份
        name,value=line.strip().split('=',1)
        newcookies[name]=value  #为字典newcookies添加内容
    
    time1 = time.time()
    
    for uid in uid_list:
        video_danmus_save(uid, newcookies)


    time2 = time.time()
    print('danmus saving all done ' + 'total_time:' + str(time2 - time1))
