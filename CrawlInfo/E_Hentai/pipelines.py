# -*- coding: utf-8 -*-

import re
import sqlite3

from E_Hentai.settings import DATASTORE

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class ItemClearPipeline(object):
    def process_item(self, item, spider):
        print '='*60
        print 'ItemClearPipeline cleaning item...'

        # favorited
        if item['favorited'] == 'Never':
            item['favorited'] = 0
        elif item['favorited'] == 'Once':
            item['favorited'] = 1
        else:
            item['favorited'] = int(item['favorited'].replace(' times', ''))

        # parent
        if 'None' in item['parent']:
            item['parent'] = -1
        else:
            item['parent'] = int(re.match('.*<a href=".+">(\d+)</a>.*', item['parent']).group(1))

        # visible
        if item['visible'] == 'Yes':
            item['visible'] = True
        else:
            item['visible'] = False

        # artist
        if 'artist' in item['features']:
            item['artist'] = item['features']['artist'][0]
        else:
            item['artist'] = 'UNKNOW'

        # file_size
        if 'MB' in item['file_size']:
            item['file_size'] = float(item['file_size'].replace(' MB', ''))
        elif 'KB' in item['file_size']:
            item['file_size'] = float(item['file_size'].replace(' KB', '')) * 0.001
        elif 'GB' in item['file_size']:
            item['file_size'] = float(item['file_size'].replace(' GB', '')) * 1000

        # parody
        if 'parody' in item['features']:
            item['parody'] = item['features']['parody'][0]
        else:
            item['parody'] = "UNKNOW"

        item['id_'] = int(item['id_'])
        item['language'] = item['language'].replace(u' \xa0', '')
        item['length'] = int(item['length'].replace(' pages', ''))
        item['posted'] = re.sub(' \d+:\d+$', '', item['posted'])
        item['rating'] = float(item['rating'].replace('Average: ', ''))
        item['rating_count'] = int(item['rating_count'])

        return item

class SqlitePipeline(object):
    '''store data to sqlite database'''
    def __init__(self):
        print("@"*60)
        print("create database at %s"%DATASTORE)
        self.conn = sqlite3.connect(DATASTORE)
        self.cur  = self.conn.cursor()
        self.init_db()

        self.features_counter = {
            'misc':         {'set': set(), 'count': 0}, 
            'character':    {'set': set(), 'count': 0}, 
            'male':         {'set': set(), 'count': 0}, 
            'female':       {'set': set(), 'count': 0}, 
        }

    def process_item(self, item, spider):
        print("v"*60)
        print("store item to database...")
        self.insert_item(item)
        for fea in ['misc', 'character', 'male', 'female']:
            if fea in item['features']:
                self.insert_feature(item, fea)
        self.conn.commit()

    def close_spider(self, spider):
        print('o'*60)
        print("spider closed")
        print("close database...")
        self.conn.close()

    def insert_feature(self, item, fea):
        for i in item['features'][fea]:
            if i not in self.features_counter[fea]['set']:
                print('insert %s: %s...'%(fea, i))
                self.features_counter[fea]['set'].add(i)
                self.features_counter[fea]['count'] += 1
                self.cur.execute('INSERT INTO %ss(id, name) VALUES(?, ?);'%fea,
                        (self.features_counter[fea]['count'], i))
            f_id = self.get_fea_id(fea, i)
            self.cur.execute('INSERT INTO item_%s(item_id, %s_id) VALUES(?, ?)'%(fea, fea),
                    (item['id_'], f_id))

    def get_fea_id(self, fea, name):
        self.cur.execute('SELECT id FROM %ss WHERE name = "%s";'%(fea, name))
        return self.cur.fetchone()[0]
        
    def insert_item(self, item):
        self.cur.execute('''INSERT INTO items
                (id, class, name, url, artist, parody, date, visible, parent, language, favorited, rating, rating_count)
                values
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);''', 
                (item['id_'], item['class_'], item['name'], item['url'], item['artist'],
                    item['parody'], item['posted'], item['visible'], item['parent'], item['language'], 
                    item['favorited'], item['rating'], item['rating_count']))

    def init_db(self):
        # items
        self.conn.execute('''CREATE TABLE items(
            id           INT         PRIMARY KEY    NOT NULL,
            class        TEXT                       NOT NULL,
            name         TEXT                       NOT NULL,
            url          TEXT                       NOT NULL,
            artist       TEXT    DEFAULT "UNKNOW",
            parody       TEXT    DEFAULT "UNKNOW",
            date         DATE,
            visible      BOOLEAN,
            parent       INT,
            language     TEXT,
            favorited    INT,
            rating       REAL,
            rating_count INT
        );''')

        # characters
        self.conn.execute('''CREATE TABLE characters(
            id      INT     PRIMARY KEY     NOT NULL,
            name    TEXT    NOT NULL
        );''')

        # item-characer
        self.conn.execute('''CREATE TABLE item_character(
            item_id     INT     NOT NULL,
            character_id INT     NOT NULL,
            PRIMARY KEY     (item_id, character_id)
        );''')

        # miscs
        self.conn.execute('''CREATE TABLE miscs(
            id      INT     PRIMARY KEY     NOT NULL,
            name    TEXT    NOT NULL
        );''')

        # item-misc
        self.conn.execute('''CREATE TABLE item_misc(
            item_id     INT     NOT NULL,
            misc_id INT     NOT NULL,
            PRIMARY KEY     (item_id, misc_id)
        );''')

        # female
        self.conn.execute('''CREATE TABLE females(
            id      INT     PRIMARY KEY     NOT NULL,
            name    TEXT    NOT NULL
        );''')

        # item-female
        self.conn.execute('''CREATE TABLE item_female(
            item_id     INT     NOT NULL,
            female_id INT     NOT NULL,
            PRIMARY KEY     (item_id, female_id)
        );''')

        # male
        self.conn.execute('''CREATE TABLE males(
            id      INT     PRIMARY KEY     NOT NULL,
            name    TEXT    NOT NULL
        );''')

        # item-male
        self.conn.execute('''CREATE TABLE item_male(
            item_id     INT     NOT NULL,
            male_id INT     NOT NULL,
            PRIMARY KEY     (item_id, male_id)
        );''')

        self.conn.commit()
