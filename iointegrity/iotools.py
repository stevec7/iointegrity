import hashlib
import os
import tempfile
import pickle
import sys

class BlockMD5(object):
    def __init__(self):
        return None

    def create_map(self, name):
        info = os.stat(name)
        left = info.st_size
        fd = os.open(name, os.O_RDONLY)
        offset = 0
        map = {}
        while left > 0:
            buf = os.read(fd, info.st_blksize)
            left -= len(buf)
            h5 = hashlib.md5(buf)
            map[offset] = h5.hexdigest()
            offset += len(buf)
        os.close(fd)
        return map

    def validate_map(self, name, map):
        failed = []
        info = os.stat(name)
        fd = os.open(name, os.O_RDONLY+os.O_DIRECT)
        left = info.st_size
        offset = 0
        while left > 0:
            buf = os.read(fd, info.st_blksize)
            left -= len(buf)
            h5 = hashlib.md5(buf)
            digest = h5.hexdigest()
            #if h5.hexdigest() != map[offset]:
            if digest != map[offset]:
                #print "failure: {0}".format(offset)
                failed.append(offset, digest, map[offset])
            offset += len(buf)
        os.close(fd)
        if len(failed) > 0:
            return False, failed
        else:
            return True

class FileMD5(object):
    def __init__(self):
        return None

    def validate_md5(self, name, md5sum):
        with open(name, 'r') as f:
            current_md5 = hashlib.md5(f.read()).hexdigest()

        if current_md5 != md5sum:
            return False, (current_md5, md5sum)
        else:
            return True
