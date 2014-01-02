import hashlib
import os
import tempfile
import pickle
import sys

class BlockMD5(object):
    def __init__(self):
        return None

    def compareblocks(offset, name1, name2):
        '''compare two files byte-by-byte'''
        info = os.stat(name1)
        fd1 = os.open(name1, os.O_RDONLY)
        fd2 = os.open(name2, os.O_RDONLY)
        os.lseek(fd1, offset, os.SEEK_SET)
        os.lseek(fd2, offset, os.SEEK_SET)
        buf1 = os.read(fd1, info.st_blksize)
        buf2 = os.read(fd2, info.st_blksize)
        os.close(fd1)
        os.close(fd2)
        for i in range(0, info.st_blksize):
            if buf1[i] != buf2[i]:
                print "Mismatch at byte_num '{0}': {1}, {2}".format(
                        i, buf1[i], buf2[i])
        return

    def createmap(self, name):
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

    def validatemap(self, name, map):
        failed = []
        info = os.stat(name)
        fd = os.open(name, os.O_RDONLY)
        #fd = os.open(name, os.O_RDONLY+os.O_DIRECT)
        left = info.st_size
        offset = 0
        while left > 0:
            buf = os.read(fd, info.st_blksize)
            left -= len(buf)
            h5 = hashlib.md5(buf)
            digest = h5.hexdigest()
            if digest != map[offset]:
                failed.append((offset, digest, map[offset]))
            offset += len(buf)
        os.close(fd)
        if len(failed) > 0:
            return False, failed
        else:
            return True

class FileMD5(object):
    def __init__(self):
        return None

    def validatemd5(self, name, md5sum):
        with open(name, 'r') as f:
            current_md5 = hashlib.md5(f.read()).hexdigest()

        if current_md5 != md5sum:
            return False, (current_md5, md5sum)
        else:
            return True
