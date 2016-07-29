# -*- coding: utf-8 -*-

from scrapy.conf import settings

class ProxyMiddleware(object):
    def process_request(self, request, spider):
        request.meta['proxy'] = settings.get('HTTP_PROXY')
