import aiohttp
import asyncio

async def danmu_xml_get(url, cookies):
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

url = 'https://api.bilibili.com/x/v2/dm/history?type=1&oid=33229541&date=2018-06-10'


cookie_raw = input('cookies?')

cookies={}
for line in cookie_raw.split(';'):   #按照字符：进行划分读取
    #其设置为1就会把字符串拆分成2份
    name,value=line.strip().split('=',1)
    cookies[name]=value  #为字典cookies添加内容

asyncio.set_event_loop(asyncio.new_event_loop())
loop = asyncio.get_event_loop()
tasks = asyncio.gather(danmu_xml_get(url, cookies))

x = loop.run_until_complete(tasks)

print(x)
