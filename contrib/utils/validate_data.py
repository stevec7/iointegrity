#!/usr/bin/env python
import datetime
import logging
import marshal
from optparse import OptionParser
from iointegrity.iotools import BlockMD5, FileMD5
from iointegrity.dbtools import IOIntegrityDB


def main(options, args):
    # setup logging
    if options.verbose:
        logging.basicConfig(format='%(message)s', level=logging.DEBUG)
        loglvl = 'verbose'
    else:
        logging.basicConfig(format='%(message)s', level=logging.INFO)
        loglvl = 'info'

    iodb = IOIntegrityDB(options.database)
    blk = BlockMD5()
    fmd5 = FileMD5(loglvl)
    date = datetime.datetime.now().isoformat()

    # get all file data from the database
    data = iodb.get_all_file_info()

    logging.debug("DEBUG: Checking data integrity...")
    for d in data:
        name = d['name']
        md5sum = d['md5sum']

        logging.debug("DEBUG: Validating {0}...".format(name))
        is_clean = fmd5.validate_md5(name, md5sum)

        if is_clean is not True:
            logging.debug("DEBUG: File '{0}' doesn't match md5sum in database".format(name))
            logging.debug("DEBUG: Doing block-level map check...")
            mapd = marshal.loads(d['chunks'])
            block_check = blk.validate_map(name, mapd)
            if block_check is not True:
                logging.debug("DEBUG: Name, Offset, Map_md5, Current_md5")
                for b in block_check[1]:
                    output = "{0},{1}".format(name, ','.join(str(e) for e in b))
                    logging.info(output)
            

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
