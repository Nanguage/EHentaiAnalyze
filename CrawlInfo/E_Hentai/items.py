# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field


class EHentaiItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    url = Field()
    id_ = Field()
    name = Field()
    class_ = Field()
    
    posted = Field()
    parent = Field()
    visible = Field()
    language = Field()
    length = Field()
    file_size = Field()

    favorited = Field()
    rating = Field()
    rating_count = Field()

    features = Field()
    artist = Field()
    parody = Field()
