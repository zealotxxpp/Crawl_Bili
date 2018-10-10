import datetime, pymysql
import time
import asyncio, aiohttp
from pandas import Series, DataFrame

config = {
          'host':'localhost',
          'port':3306,
          'user':'root',
          'password':'ms020312',
          'db':'bili',
          'charset':'utf8mb4'
          }

cookies = None
todo_timestamp_list = []
cidtodotimestamp_DF = None
aidcreattime_DF = None

def CT_video_timestamps_todo():

    global config

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = """create table video_timestamps_todo(uid varchar(20), aid varchar(20), cid varchar(20), todo_timestamp varchar(20))engine=innodb charset utf8mb4"""
            cursor.execute(sql)

        connection.commit()
    finally:
        connection.close()

async def rolldate_get(cid, dtmonth):
    global cookies
    url = 'https://api.bilibili.com/x/v2/dm/history/index?type=1&oid=%s&month=%s' % (cid, dtmonth)
    async with aiohttp.ClientSession(cookies=cookies) as session:
        try:
            async with session.get(url=url) as resp:
                text = await resp.text()

                if resp.status != 200:
                    raise NameError

                return text

        except aiohttp.client_exceptions.ClientError:
            raise NameError

def get_aidcreattime_DF(uid):
    
    global config
    connection = pymysql.connect(**config)
    cur = connection.cursor(pymysql.cursors.DictCursor)

    sql = "SELECT DISTINCT b.aid, b.createdtime FROM uper_videos b WHERE b.uid = %s"
    cur.execute(sql % uid)

    data = cur.fetchall()
    cur.close()
    connection.close()

    if len(data) == 0:
        DF = DataFrame()
        print('uid:%s has no video in Database' % uid)
        return DF

    DF = DataFrame(data)
    print('uid:%s has %s videos in Database' % (uid,len(DF)))
    return DF

def get_timestamptodo_last(cid):
    
    global config
    connection = pymysql.connect(**config)
    cur = connection.cursor(pymysql.cursors.DictCursor)

    sql = "SELECT MAX(b.todo_timestamp) FROM video_timestamps_todo b WHERE b.cid = %s GROUP BY b.cid"
    cur.execute(sql % cid)

    data = cur.fetchall()
    cur.close()
    connection.close()
    
    return data

def get_rolldate_monthlist(aid, cid):      
    
    global aidcreattime_DF
    
    data = get_timestamptodo_last(cid)
    
    if len(data) == 0:
        try:
            ctdt_numpy = aidcreattime_DF.createdtime[aidcreattime_DF.aid == str(aid)].unique()[0]
            ctdt = datetime.datetime.utcfromtimestamp(ctdt_numpy.astype(object) * (1e-9))

        except AttributeError:
            ctdt = datetime.datetime(2009, 1, 1)   
    else:
        ctdt = datetime.datetime.fromtimestamp(int(data[0].popitem()[1]))      
    
    dtlist = []
    today = datetime.datetime.today()
    
    if ctdt.year < today.year:
        for j in range(ctdt.month, 13):
            dtmonth = '{0:4d}-{1:02d}'.format(ctdt.year, j)
            dtlist.append(dtmonth)

    for i in range(ctdt.year + 1, today.year):
        for j in range(1, 13):
            dtmonth = '{0:4d}-{1:02d}'.format(i, j)
            dtlist.append(dtmonth)
            
    if ctdt.year == today.year:
        for j in range(ctdt.month, today.month + 1):
            dtmonth = '{0:4d}-{1:02d}'.format(today.year, j)
            dtlist.append(dtmonth)
    else:      
        for j in range(1, today.month + 1):
            dtmonth = '{0:4d}-{1:02d}'.format(today.year, j)
            dtlist.append(dtmonth)
    
    return dtlist

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

async def rolldate_coll(uid, aid, cid):
    global todo_timestamp_list
    global cidtodotimestamp_DF
    
    url_temp = []

    try:
        timestamps = cidtodotimestamp_DF.todo_timestamp[cidtodotimestamp_DF.cid == str(cid)].unique()
    except AttributeError:
        timestamps = []

    monthlist = get_rolldate_monthlist(aid, cid)
    
    i = 0
    collnum = 0
    while i < len(monthlist):
        url_temp = []
        try:
            text = await rolldate_get(cid, monthlist[i])
        except NameError:
            await asyncio.sleep(1)
            continue
        textdict = eval(text)
        if textdict['code'] != 0:
            print (textdict)
            raise ConnectionRefusedError

        datelist = textdict['data']

        for date in datelist:
            todo_timestamp = int(datetime.datetime.strptime(date, '%Y-%m-%d').timestamp())
            if str(todo_timestamp) in timestamps:
                continue
            temp = [uid, aid, cid, todo_timestamp]
            collnum = collnum + 1
            url_temp.append(temp)

        todo_timestamp_list.extend(url_temp)
        i = i + 1   

    print('uid: %s aid: %s cid: %s has %s rolldate, been collected' % (uid, aid, cid, collnum))

def video_timestampstodo_insert(todo_timestamp_list):

    global config

    connection = pymysql.connect(**config)
    try:
        with connection.cursor() as cursor:

            sql = """INSERT INTO video_timestamps_todo(uid,aid,cid,todo_timestamp)VALUES(%s,%s,%s,%s);"""

            try:
                cursor.executemany(sql,todo_timestamp_list)
                connection.commit()
                print('INSET %d ROWS in Table todo_timestamp_list' % len(todo_timestamp_list))
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

def update_todotimestamplist(uid, newcookies):
    
    global todo_timestamp_list
    global cookies
    global cidtodotimestamp_DF
    global aidcreattime_DF

    cookies = newcookies
    cidtodotimestamp_DF = get_cidtodotimestamp_DF(uid)
    aidcreattime_DF = get_aidcreattime_DF(uid)

    cidlist = get_cid_list(uid)

    cidlist_num = len(cidlist)
    roundnum = (cidlist_num // 5) + 1
    
    st = 0
    while st < roundnum:
        start = st * 5
        end = start + 5
        todo_timestamp_list = []

        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        tasks = asyncio.gather(*[rolldate_coll(uid, aid, cid) for [aid, cid] in cidlist[start:end]])
        st = st + 1
        try:
            loop.run_until_complete(tasks)
        except ConnectionRefusedError:
            print('ConnectionRefusedError')
            loop.close()
            time.sleep(60)
            st = st - 1
            continue
            
        loop.close()
        video_timestampstodo_insert(todo_timestamp_list)
        time.sleep(120)



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
        update_todotimestamplist(uid, newcookies)


    time2 = time.time()
    print('update video_todo all done ' + 'total_time:' + str(time2 - time1))