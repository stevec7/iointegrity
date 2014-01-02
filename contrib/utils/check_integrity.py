#!/usr/bin/env python
import datetime
import hashlib
import optparse
import os
import sys


def main(options, args):
    f = open(options.md5sumfile, 'r')
    md5sums = {}
            
    start_date = datetime.datetime.now()
    print "*"*40
    print "Starting another iteration at: {0}".format(start_date.isoformat())

    for line in f:
        k = line.split()[1]
        v = line.split()[0]
        md5sums[k] = v

    for root, dirs, files in os.walk(options.filedir):
        for file in files:
            d = datetime.datetime.now()
            j = open(root+"/"+file, 'r')
            s = hashlib.md5(j.read()).hexdigest()
            #s = hashlib.md5(open(root+"/"+file).read()).hexdigest()
            if s == md5sums[file]:
                pass
            else:
                print "{0} : File '{1}' checksum '{2}' does not match recorded checksum '{3}'".format(
                        d.isoformat(), file, s, md5sums[file])


    
if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("--md5sumfile",
                      dest="md5sumfile",
                      help="the file containing the md5sums")
    parser.add_option("--filedir",
                      dest="filedir",
                      help="the location of the files from the md5sumfile")
    (options, args) = parser.parse_args()
    main(options, args)
