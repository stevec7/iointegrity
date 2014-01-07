import hashlib
import os
import tempfile
import pickle
import sys

class BlockMD5(object):
    def __init__(self):
        return None

    def compare_blocks(offset, name1, name2):
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

    def create_map(self, name):
        '''Create a per block md5sum of a file
           and return a dict of block->md5hashes'''
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
        '''Compares the block md5sums to each block of the file'''
        failed = []
        info = os.stat(name)
        fd = os.open(name, os.O_RDONLY)
        # O_DIRECT didn't work on my test system, but worked on a GPFS filesystem
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

    def create_md5(self, name):
        with open(name, 'r') as f:
            md5sum = hashlib.md5(f.read()).hexdigest()

        return md5sum

    def validate_md5(self, name, md5sum):
        logging.debug("Opening '{0}' to check md5sum".format(name))
        print "Opening '{0}' to check md5sum".format(name)
        with open(name, 'r') as f:
            current_md5 = hashlib.md5(f.read()).hexdigest()

        if current_md5 != md5sum:
            return False, (current_md5, md5sum)
        else:
            return True

def create_random_file(name, numbytes):
    '''writes out a file full of random data'''
    path = os.path.dirname(os.path.abspath(name))
    vfsstats = os.statvfs(path)

    # dont write the file if there isn't enough free space on the filesystem
    if numbytes > (vfsstats.f_ffree * vfsstats.f_bfree):
        print "Not enough space to write data."
        return

    bufsize = vfsstats.f_bsize

    if numbytes % bufsize != 0:
        print "Number of bytes must be a multiple of blocksize ({0})".format(
                bufsize)
        return

    bytes_left = numbytes

    with open(name, 'wb') as f:
        while bytes_left > 0: 
            f.write(os.urandom(bufsize))
            bytes_left -= bufsize
    return
