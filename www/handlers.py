#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Michael Liao'

' url handlers '
from coroweb import get,post
from aiohttp import web

@get('/')
def index(request):
    return web.Response(body=b'<h1>Awsome</h1>',content_type='text/html')
    