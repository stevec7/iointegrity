#!/usr/bin/env python
import datetime
import os
import sqlite3 as sql
import sys
from optparse import OptionParser

def main(options, args):

    conn = sql.connect(options.dbfile)
    c = conn.cursor()
    datestr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # file format:
    # [scrusan@sallystruthers files]$ head -n 2 ../md5sum_list 
    # 7340d12e4e511d8bf6b8e2eb5e5c1b0a  /home/scrusan/workspace/python/tmp/sqlite/files/file10.dat
    # 9ea3cf62942d08cae722bc846b28f94c  /home/scrusan/workspace/python/tmp/sqlite/files/file1.dat
    with open(options.md5file, 'r') as f:
        data = [ (l.split()[1], l.split()[0], datestr) for l in f.readlines() ]

    # insert md5sums into the database
    c.executemany('INSERT INTO md5sums VALUES(?, ?, ?)', data)
    conn.commit()
    conn.close()



if __name__ == '__main__':
    parser = OptionParser('Import md5sums into the database')
    parser.add_option('--dbfile',
                    dest='dbfile',
                    help='database file to add entries to.')
    parser.add_option('--md5file',
                    dest='md5file',
                    help='file containing md5sums, EX: md5sumval    full_path')
    options, args = parser.parse_args()
    main(options, args)
