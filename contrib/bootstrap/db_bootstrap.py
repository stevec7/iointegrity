#!/usr/bin/env python
import os
import sqlite3 as sql
import sys
from optparse import OptionParser

def main(options, args):

    conn = sql.connect(options.dbfile)
    c = conn.cursor()
    c.execute('''CREATE TABLE fileintegrity
                (name text, md5sum text, chunks blob, 
                 fileloc blob, date_added date)''')
    conn.commit()
    conn.close()



if __name__ == '__main__':
    parser = OptionParser('Creates the necessary db tables')
    parser.add_option('-f', '--dbfile',
                    dest='dbfile',
                    help='database file to create tables in')
    options, args = parser.parse_args()
    main(options, args)
