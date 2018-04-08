import requests, re, datetime, pymysql
import json as js
import time
import asyncio, aiohttp

def CT_uper_baseinfo():

    config = {
          'host':'localhost',
          'port':3306,
          'user':'root',
          'password':'ms020312',
          'db':'test',
          'charset':'utf8mb4'
          }

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = """create table uper_baseinfo(uid varchar(20),
            mid varchar(20), name varchar(50), approve bool, sex varchar(10), rank int, face_url varchar(100), DisplayRank int,
            regtime datetime, spacesta int, birthday_mmdd datetime, place varchar(20), description varchar(50), article int,
            sign varchar(200), level_current_level int, level_current_min int, level_current_exp bigint, pendant_pid int,
            pendant_name varchar(20), pendant_image_url varchar(100), pendant_expire datetime, nameplate_nid int,
            nameplate_name varchar(20), nameplate_image_url varchar(100), nameplate_level varchar(20), nameplate_condition varchar(50),
            official_verify_type int, official_verify_desc varchar(100), vip_vipType int, vip_vipDueDate datetime, vip_dueRemark varchar(20),
            vip_accessStatus int, vip_vipStatus int, vip_vipStatusWarn varchar(20), toutu_url varchar(200), toutuId int, theme varchar(20),
            theme_preview varchar(20), coins int, im9_sign varchar(50), playNum bigint, fans_badge bool, record_time datetime,
            baseinfo_source varchar(100)
            )
            engine=innodb charset utf8mb4"""
            cursor.execute(sql)

        connection.commit()
        print ('table up_baseinfo been created')
    finally:
        connection.close()

def uper_baseinfo_get(uid):

    headers = {'Host': 'space.bilibili.com',
                'Connection': 'keep-alive',
                'Content-Length': '18',
                'Accept': '*/*',
                'Origin': 'https://space.bilibili.com',
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Referer': 'https://space.bilibili.com/%d'% uid,
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,zh-TW;q=0.8'
               }

    postload = {'mid': '%d' % uid, 'csrf': 'null'}

    response = requests.post(url='https://space.bilibili.com/ajax/member/GetInfo', headers=headers, data=postload)
    response.encoding = 'utf-8'
    text = response.text

    return text

def uper_baseinfo_insert(uid, text):


    jstext = js.loads(text,encoding='utf8')
    data = jstext['data']
    status = jstext['status']

    if status == False:
        print('NO such uper, uid:%s' % uid)
        return

    config = {
          'host':'localhost',
          'port':3306,
          'user':'root',
          'password':'ms020312',
          'db':'test',
          'charset':'utf8mb4'
          }

    connection = pymysql.connect(**config)

    try:
        with connection.cursor() as cursor:
            now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            uid = uid
            mid = data['mid']
            name = data['name']
            approve = data['approve']
            sex = data['sex']
            rank = data['rank']
            face_url = data['face']
            DisplayRank = data['DisplayRank']

            try:
                regtime = datetime.datetime.fromtimestamp(max(data['regtime'],86400)).strftime('%Y-%m-%d %H:%M:%S')
            except KeyError as e:
                regtime = None
                print('KeyError:%s' % e)

            spacesta = data['spacesta']

            try:
                birthday_mmdd = data['birthday']
            except KeyError as e:
                birthday_mmdd = None
                print('KeyError:%s' % e)

            try:
                place = data['place']
            except KeyError as e:
                place = None
                print('KeyError:%s' % e)

            description = data['description']
            article = data['article']
            sign = data['sign']
            level_current_level = data['level_info']['current_level']
            level_current_min = data['level_info']['current_min']
            level_current_exp = data['level_info']['current_exp']
            pendant_pid = data['pendant']['pid']
            pendant_name = data['pendant']['name']
            pendant_image_url = data['pendant']['image']
            pendant_expire = datetime.datetime.fromtimestamp(max(data['pendant']['expire'], 86400)).strftime('%Y-%m-%d %H:%M:%S')
            nameplate_nid = data['nameplate']['nid']
            nameplate_name = data['nameplate']['name']
            nameplate_image_url = data['nameplate']['image']
            nameplate_level = data['nameplate']['level']
            nameplate_condition = data['nameplate']['condition']
            official_verify_type = data['official_verify']['type']
            official_verify_desc = data['official_verify']['desc']
            vip_vipType = data['vip']['vipType']
            vip_vipDueDate = datetime.datetime.fromtimestamp(max(data['vip']['vipDueDate']/1000, 86400)).strftime('%Y-%m-%d %H:%M:%S')
            vip_dueRemark = data['vip']['dueRemark']
            vip_accessStatus = data['vip']['accessStatus']
            vip_vipStatus = data['vip']['vipStatus']
            vip_vipStatusWarn = data['vip']['vipStatusWarn']
            toutu_url = 'https://i0.hdslb.com/%s' % data['toutu']
            toutuId = data['toutuId']
            theme = data['theme']
            theme_preview = data['theme_preview']
            coins = data['coins']
            im9_sign = data['im9_sign']
            playNum = data['playNum']
            fans_badge = data['fans_badge']
            record_time = now_str
            baseinfo_source = 'https://space.bilibili.com/%d'% uid



            sql = """INSERT INTO uper_baseinfo(
            uid,mid,name,approve,sex,rank,face_url, DisplayRank,regtime , spacesta , birthday_mmdd , place , description , article ,
            sign , level_current_level , level_current_min , level_current_exp , pendant_pid ,
            pendant_name , pendant_image_url , pendant_expire , nameplate_nid ,
            nameplate_name , nameplate_image_url , nameplate_level , nameplate_condition ,
            official_verify_type , official_verify_desc , vip_vipType , vip_vipDueDate , vip_dueRemark ,
            vip_accessStatus , vip_vipStatus , vip_vipStatusWarn , toutu_url , toutuId , theme ,
            theme_preview , coins , im9_sign , playNum , fans_badge , record_time , baseinfo_source )
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s);"""

            cursor.execute(sql,(
            uid,mid,name,approve,sex,rank,face_url, DisplayRank,regtime , spacesta , birthday_mmdd , place , description , article ,
            sign , level_current_level , level_current_min , level_current_exp , pendant_pid ,
            pendant_name , pendant_image_url , pendant_expire , nameplate_nid ,
            nameplate_name , nameplate_image_url , nameplate_level , nameplate_condition ,
            official_verify_type , official_verify_desc , vip_vipType , vip_vipDueDate , vip_dueRemark ,
            vip_accessStatus , vip_vipStatus , vip_vipStatusWarn , toutu_url , toutuId , theme ,
            theme_preview , coins , im9_sign , playNum , fans_badge , record_time , baseinfo_source))

        connection.commit()
        print('INSET 1 ROWS in Table uper_baseinfo, uid:%s mid:%s name:%s regtime:%s' % (uid,mid,name,regtime))

    finally:
        connection.close()


def uper_baseinfo_save(uid):
    time1 = time.time()
    text = uper_baseinfo_get(uid)
    time2 = time.time()
    print('text_get done uid:' + str(uid) + 'time:' + str(time2 - time1))


    uper_baseinfo_insert(uid, text)
    time3 = time.time()
    print('info_insert done uid:' + str(uid) + 'time:' + str(time3 - time2) + 'total_time:' + str(time3 - time1))


if __name__ == '__main__':
    time1 = time.time()
    for i in range(30,40):
        uper_baseinfo_save(i)
    time2 = time.time()
    print('info_save all done ' + 'total_time:' + str(time2 - time1))
