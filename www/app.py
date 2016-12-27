import logging;logging.basicConfig(level=logging.INFO)

import asyncio,os,json,time

from aiohttp import web

def index(request):
    return web.Response(body=b'<h1>Awsome</h1>',content_type='text/html')
#@asyncio.coroutine把一个generator标记为coroutine类型，然后，
#我们就把这个coroutine扔到EventLoop中执行。
@asyncio.coroutine
def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET','/',index)
    srv = yield from loop.create_server(app.make_handler(),'127.0.0.1',9000)
    logging.info('server started at http://127.0.0.1:9000')
    return srv
#获取EventLoop    
loop = asyncio.get_event_loop()
#执行coroutine
loop.run_until_complete(init(loop))
loop.run_forever()

