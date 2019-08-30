#!/usr/bin/env python
import datetime
import os
import marshal
import sqlite3 as sql
import sys
from optparse import OptionParser
from iointegrity.iotools import BlockMD5

def main(options, args):

    conn = sql.connect(options.dbfile)
    c = conn.cursor()
    datestr = datetime.datetime.now().isoformat()
    data = []

    bmd5 = BlockMD5()


    # Need a few things here...
    #
    # 1.) md5sums of each file
    # file format:
    # [username@sallystruthers files]$ head -n 2 ../md5sum_list 
    # 7340d12e4e511d8bf6b8e2eb5e5c1b0a  /home/username/workspace/python/tmp/sqlite/files/file10.dat
    # 9ea3cf62942d08cae722bc846b28f94c  /home/username/workspace/python/tmp/sqlite/files/file1.dat
    with open(options.md5file, 'r') as f:
        pairs = [ (l.split()[0], l.split()[1]) for l in f.readlines() ]

    # 2.) block maps of each file
    for p in pairs:
        m = marshal.dumps(bmd5.create_map(p[1]))
        data.append((p[1], p[0], m, '', datestr))

    # 3.) on disk location of data
    # TODO

    # insert all of it into the database
    c.executemany('INSERT INTO fileintegrity VALUES(?, ?, ?, ?, ?)', data)
    conn.commit()
    conn.close()



if __name__ == '__main__':
    parser = OptionParser('Import initial data into the database')
    parser.add_option('--dbfile',
                    dest='dbfile',
                    help='database file to add entries to.')
    parser.add_option('--md5file',
                    dest='md5file',
                    help='file containing md5sums, EX: md5sumval    full_path')
    options, args = parser.parse_args()
    main(options, args)
