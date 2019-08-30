#!/usr/bin/env python
#
#
import marshal
from iointegrity.dbtools import IOIntegrityDB
from optparse import OptionParser

def main(options, args):
    db = IOIntegrityDB(options.database)
    entries = db.get_all_file_info()
    for entry in entries:
        print(dict(entry))

    return

if __name__ == '__main__':
    parser = OptionParser('Validate files from a database.')
    parser.add_option('-d','--db',
                        dest='database',
                        help='database file.')
    parser.add_option('-v','--verbose',
                        action='store_true',
                        dest='verbose',
                        help='print verbose statements')
    options, args = parser.parse_args()
    main(options, args)
