import datetime
import hashlib
import marshal
import os
import sqlite3 as sql
import sys
import tempfile

class IOIntegrityDB(object):
    '''The only function of this class is to send and retrieve
       data from the database. Nothing more...'''
    def __init__(self, dbname, tablename='fileintegrity'):
        # setup db connections
        self.dbtable = tablename
        self.sqlconn = sql.connect(dbname)
        self.sqlconn.text_factory = str
        self.sqlconn.row_factory = sql.Row # return dicts of queries
        self.sqlrun = self.sqlconn.cursor()
        return None

    def __enter__(self):
        return self

    def __exit__(self):
        # if object used with the 'with' statement, 
        #   automatically close the db connection
        self.sqlconn.close()

    def get_all_file_info(self):
        '''returns a dict of all files in the database'''
        query = 'SELECT * FROM {0}'.format(self.dbtable)
        self.sqlrun.execute(query)
        data = self.sqlrun.fetchall()
        return data

    def get_file_info(self, name):
        '''returns a dict of single file data'''
        query = 'SELECT * FROM {0} WHERE name=?'.format(self.dbtable)
        self.sqlrun.execute(query, name)
        data = self.sqlrun.fetchone()
        return data

    def get_attr(self, name, attr):
        '''Gets the specified attribute for a file
           and returns it'''
        query = 'SELECT {0} FROM {1} WHERE name=?'.format(attr, self.dbtable)
        self.sqlrun.execute(query, name)
        result = self.sqlrun.fetchone()
        mapd = marshal.loads(result)

        return mapd

    def mass_insert_filemd5(self, data):
        self.sqlrun.executemany('INSERT INTO fileintegrity VALUES(?, ?, ?, ?, ?)', data)
        self.sqlconn.commit()
        return

    def write_blockmd5_to_db(self, name, mapd):
        '''Writes map data to the database'''
        block_data = marshal.dumps(mapd)
        date = datetime.datetime.now().isoformat()
        data = (name, block_data, date)
        self.sqlrun.execute('INSERT INTO blockmd5 VALUES(?, ?, ?)', data)
        self.sqlconn.commit()
        return

    def write_filemd5_to_db(self, name, md5sum):
        '''Write file md5sum to the database'''
        date = datetime.datetime.now().isoformat()
        data = (name, md5sum, date)
        self.sqlrun.execute('INSERT INTO filemd5 VALUES(?, ?, ?)', data)
        self.sqlconn.commit()
        return

    def _smoochy(self):
        return 'DEATH TO HIM!!!'
