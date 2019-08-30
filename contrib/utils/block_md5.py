#!/usr/bin/env python
import hashlib
import os
import tempfile
import pickle
import sys

def load_map (name):
    f = open(name)
    mapd = pickle.load(f)
    f.close()
    return mapd

def store_map(mapd, name):
    f = open(name, 'w')
    pickle.dump(mapd, f)
    f.close()
    return

def create_map(name):
    info = os.stat(name)
    left = info.st_size
    fd = os.open(name, os.O_RDONLY)
    offset = 0
    mapd = {}
    while left > 0:
        buf = os.read(fd, info.st_blksize)
        left -= len(buf)
        h5 = hashlib.md5(buf)
        mapd[offset] = h5.hexdigest()
        offset += len(buf)
    os.close(fd)
    return mapd

def validate_map(name, mapd):
    info = os.stat(name)
    fd = os.open(name, os.O_RDONLY+os.O_DIRECT)
    left = info.st_size
    offset = 0
    while left > 0:
        buf = os.read(fd, info.st_blksize)
        left -= len(buf)
        h5 = hashlib.md5(buf)
        if h5.hexdigest() != mapd[offset]:
            print("failure: {0}".format(offset))
        offset += len(buf)
    os.close(fd)
    return

def compare_block (offset, name1, name2):
    info = os.stat(name1)
    #fd1 = os.open(name1, os.O_RDONLY+os.O_DIRECT)
    fd1 = os.open(name1, os.O_RDONLY)
    #fd2 = os.open(name2, os.O_RDONLY+os.O_DIRECT)
    fd2 = os.open(name2, os.O_RDONLY)
    os.lseek(fd1, offset, os.SEEK_SET)
    os.lseek(fd2, offset, os.SEEK_SET)
    buf1 = os.read(fd1, info.st_blksize)
    buf2 = os.read(fd2, info.st_blksize)
    os.close(fd1)
    os.close(fd2)
    for i in range(0, info.st_blksize):
        if buf1[i] != buf2[i]:
            print(i, buf1[i], buf2[i])
    
def main(options, args):
    if options.createmap:
        if not options.verifymap:
            print("If you are just creating a mapfile, you need to supply a file to create one with, via --verify")
            sys.exit(1)

        mapblob = create_map(options.verifymap)
        store_map(mapblob, options.createmap)
        sys.exit(0)
        

    if not options.readmap:
	    # make a tmpfile to use
        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        mapblob = create_map(options.verifymap)
        store_map(mapblob, tmpfile.name)
        mapfile = tmpfile.name
    else:
        mapfile = options.readmap

    mapd = load_map(mapfile)
    validate_map(options.verifymap, mapd)
    return

if __name__ == '__main__':
    import optparse
    parser = optparse.OptionParser()
    parser.add_option("--createmap",
                      dest="createmap",
                      help="just create the mapfile, and then exit. This will be the path")
    parser.add_option("--readmap",
                      dest="readmap",
                      help="the file mapping. if not supplied, it will be generated")
    parser.add_option("--verifymap",
                      dest="verifymap",
                      help="the file to verify blocks"),
    (options, args) = parser.parse_args()
    main(options, args)
