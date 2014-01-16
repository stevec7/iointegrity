import hashlib
import logging
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
    def __init__(self, loglvl='info'):
        if loglvl == 'verbose':
            logging.basicConfig(format='%(message)s', level=logging.DEBUG)
        else:
            logging.basicConfig(format='%(message)s', level=logging.INFO)
        return None

    def create_md5(self, name):
        with open(name, 'r') as f:
            md5sum = hashlib.md5(f.read()).hexdigest()

        return md5sum

    def validate_md5(self, name, md5sum):
        logging.debug("DEBUG: FileMD5().validate_md5({0}, {1})".format(name, md5sum))
        with open(name, 'r') as f:
            current_md5 = hashlib.md5(f.read()).hexdigest()

        if current_md5 != md5sum:
            return False, (current_md5, md5sum)
        else:
            return True

class FileTree(object):
    def __init__(self, topdir, num_top_dirs=1, num_sub_dirs=0, 
            third_level_dirs=0, files_per_dir=1, max_size=8192, 
            aligned=True, stats=False, suffix=''):
        '''Set the configuration'''
        self.root = topdir
        vfsstats = os.statvfs(self.root)
        self.bufsize = vfsstats.f_bsize
        self.num_top_dirs = num_top_dirs
        self.num_sub_dirs = num_sub_dirs
        self.max_size = max_size
        self.files_per_dir = files_per_dir
        self.aligned = aligned
        self.stats = stats
        self.suffix = suffix

        return None

    def _free_space(self, num_bytes):
        '''Checks to see if there is enough space on the filesystem'''
        vfsstats = os.path.dirname(os.path.abspath(self.root))

        if num_bytes > (vfsstats.f_ffree * vfsstats.f_bfree):
            return False
        
        return True

    def _gen_all_dirs(self):
        '''Create the directory hierarchy all at once'''
        # make an array of directory paths
        self.dirs = []

        # if I was cool, I'd do some crazy algorithm or list comprehension
        for topdir in range(0, self.num_top_dirs):
            dirname = self.root + '/' + str(topdir)
            self.dirs.append(dirname)
            for subdir in range(0, self.num_sub_dirs):
                dirname = self.root + '/' + str(topdir) + '/'+str(subdir)
                self.dirs.append(dirname)
                for subsubdir in range(0, self.third_level_dirs):
                    dirname = self.root + '/' + str(topdir) + '/' + str(subdir)
                    dirname = dirname + '/' + str(subsubdir)
                    self.dirs.append(dirname)

        return dirs

    def _path_exists(self, filepath):
        '''Checks to see if the path exists'''
        if not os.path.isdir(os.path.dirname(filepath)):
            return False

        return True
       

    def write_file(self, filepath):       
        '''Create a number of random files in a directory tree of varying size'''
        # pick a random bytesize between 0 and max_size
        if self.aligned:
            num_bytes = random.randrange(self.bufsize, 
                    stop=self.max_size, step=self.bufsize)
        else:
            num_bytes = random.randrange(1, self.max_size)

        # check to see if enough space is available
        if not self._free_space(num_bytes):
            return "Not enough space to write data."
        
        # check to see if path exists
        if not self._path_exists(filepath):
            return "Directory does not exist."
        
        # figure out how many chunks we need to write
        bytes_left = num_bytes

        # add the suffix if specified
        filepath = filepath+self.suffix

        # write out the random data
        with open(filepath, 'wb') as f:
            while bytes_left > 0: 
                if bytes_left < self.bufsize:
                    f.write(os.urandom(bytes_left))
                    bytes_left -= bufsize
                else:
                    f.write(os.urandom(bufsize))
                    bytes_left -= bufsize
        return

# for when you don't want to use the FileTree class, 
#   and simply want to create a random file
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
