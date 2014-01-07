#!/usr/bin/env python
import datetime
import pickle
from optparse import OptionParser
from iointegrity.iotools import BlockMD5, FileMD5
from iointegrity.dbtools import IOIntegrityDB


def main(options, args):
    iodb = IOIntegrityDB(options.database)
    blk = BlockMD5()
    fmd5 = FileMD5()
    date = datetime.datetime.now().isoformat()

    # get all file data from the database
    data = iodb.get_all_file_info()

    print "Checking data integrity..."
    for d in data:
        name = d['name']
        md5sum = d['md5sum']

        is_clean = fmd5.validate_md5(name, md5sum)

        if is_clean is not True:
            print "File '{0}' doesn't match md5sum in database".format(name)
            print "Doing block-level map check..."
            map = pickle.marshal.loads(d['chunks'])
            block_check = blk.validate_map(name, map)
            if block_check is not True:
                print "Offset, Map_md5, Current_md5"
                for b in block_check[1]:
                    print b
            

if __name__ == '__main__':
    parser = OptionParser('Validate files from a database.')
    parser.add_option('-d','--db',
                        dest='database',
                        help='database file.')
    options, args = parser.parse_args()
    main(options, args)
