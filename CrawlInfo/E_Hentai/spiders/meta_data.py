# -*- coding: utf-8 -*-

import re

# from scrapy.utils.response import open_in_browser
# from scrapy.shell import inspect_response
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule, CrawlSpider
from scrapy.http import Request, FormRequest
from scrapy.selector import Selector

from E_Hentai.items import EHentaiItem

class MetaDataSpider(CrawlSpider):
    download_delay = 2
    name = "meta_data"
    allowed_domains = []

    start_urls = (
        'http://g.e-hentai.org/?page=0',
    )

    rules = (
        Rule(LinkExtractor(allow=(r'http://g\.e-hentai\.org/\?page=\d+')),
            follow=True),
        Rule(LinkExtractor(allow=(r'http://g\.e-hentai\.org/g/\d+/.+/')),
            callback='parse_item', follow=False),
    )

    def parse_item(self, response):
        print "*"*60
        print "parsing: " + response.url

        sel = Selector(response)

        url = response.url
        id_ = re.match('http://g\.e-hentai\.org/g/(\d+)/.+/', url).group(1)

        class_img_src = sel.xpath('//img[@class="ic"]/@src').extract()[0]
        class_ = re.match('^.+/(.+)\.png$', class_img_src).group(1)

        name = sel.xpath('//h1[@id="gn"]/text()').extract()[0]
        posted = sel.xpath('//*[@id="gdd"]/table/tr[1]/td[2]/text()').extract()[0]
        parent = sel.xpath('//*[@id="gdd"]/table/tr[2]/td[2]').extract()[0]
        visible = sel.xpath('//*[@id="gdd"]/table/tr[3]/td[2]/text()').extract()[0]
        language = sel.xpath('//*[@id="gdd"]/table/tr[4]/td[2]/text()').extract()[0]
        file_size = sel.xpath('//*[@id="gdd"]/table/tr[5]/td[2]/text()').extract()[0]
        length = sel.xpath('//*[@id="gdd"]/table/tr[6]/td[2]/text()').extract()[0]
        
        favorited = sel.xpath('//*[@id="favcount"]/text()').extract()[0]
        rating = sel.xpath('//*[@id="rating_label"]/text()').extract()[0]
        rating_count = sel.xpath('//*[@id="rating_count"]/text()').extract()[0]

        features = {}
        trs = sel.xpath('//*[@id="taglist"]/table/tr')
        for tr in trs:
            i_key = tr.xpath('.//td[1]/text()').extract()[0].strip(':')
            i_values = tr.xpath('.//td[2]//a/text()').extract()
            features[i_key] = i_values
        
        item = EHentaiItem()
        item['url'] = url
        item['id_'] = id_
        item['class_'] = class_
        item['name'] = name
        item['posted'] = posted
        item['parent'] = parent
        item['visible'] = visible
        item['language'] = language
        item['file_size'] = file_size
        item['length'] = length
        item['favorited'] = favorited
        item['rating'] = rating
        item['rating_count'] = rating_count
        item['features'] = features

        return item
