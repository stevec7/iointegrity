import hashlib
import logging
import os
import tempfile
import pickle
import random
import shutil
import string
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
                print("Mismatch at byte_num '{0}': {1}, {2}".format(
                        i, buf1[i], buf2[i]))
        return

    def create_map(self, name):
        '''Create a per block md5sum of a file
           and return a dict of block->md5hashes'''
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

    def validate_map(self, name, mapd):
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
            if digest != mapd[offset]:
                failed.append((offset, digest, mapd[offset]))
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
        with open(name, 'rb') as f:
            md5sum = hashlib.md5(f.read()).hexdigest()

        return md5sum

    def validate_md5(self, name, md5sum):
        logging.debug("DEBUG: FileMD5().validate_md5({0}, {1})".format(name, md5sum))
        with open(name, 'rb') as f:
            current_md5 = hashlib.md5(f.read()).hexdigest()

        if current_md5 != md5sum:
            return False, (current_md5, md5sum)
        else:
            return True

class FileTree(object):
    def __init__(self):
        '''Set defaults'''
        self.aligned = True
        self.dirs_per_level = 1
        self.files_per_dir = 1
        self.fixed_size = False
        self.loglvl = 'info'
        self.max_size = 8192
        self.num_levels = 1
        self.stats = False
        self.suffix = ''
        self.topdir = None

        return None

    def set_config(self, kwargs):
        '''Set class configuration'''
        for k, v in kwargs.items():
            setattr(self, k, v)

        # get the blocksize
        vfsstats = os.statvfs(self.topdir)
        self.bufsize = vfsstats.f_bsize

        # set logging
        if self.loglvl == 'verbose' or self.loglvl == 'debug':
            logging.basicConfig(format='%(message)s', level=logging.DEBUG)
        else:
            logging.basicConfig(format='%(message)s', level=logging.INFO)

        return

    def _free_space(self, num_bytes):
        '''Checks to see if there is enough space on the filesystem'''
        vfsstats = os.statvfs(os.path.dirname(os.path.abspath(self.topdir)))

        bytes_free = vfsstats.f_ffree * vfsstats.f_bfree
        logging.debug("DEBUG: Bytes_to_write: {0}, Bytes_Free: {1}".format(
                    num_bytes, bytes_free))
        if num_bytes > bytes_free:
            return False
        
        return True

    def _path_exists(self, filepath):
        '''Checks to see if the path exists'''
        if not os.path.isdir(os.path.dirname(filepath)):
            return False

        return True

    def _sub_tree(self, path, levels, dirs_per_level):
        '''should be called recursively to generate names of levels of dirs'''
        for n in range(dirs_per_level):
            dirname = "L{0}D{1}".format(levels, n)
            newdir = os.path.join(path, dirname)
            self.dirs.append(newdir)
            for nl in range(levels):
                self._sub_tree(newdir, nl, dirs_per_level)

    def gen_dir_array(self, topdir, levels, dirs_per_level):
        '''Generate the directory hierarchy array all at once
            
            I won't lie, I'm basically recreating (poor attempt anyway) 
            fdtree in Python:
            https://computing.llnl.gov/?set=code&page=sio_downloads
        '''
        # make an array of directory paths
        self.dirs = []

        # this will start recursively calling itself until
        #   you've reached the end (num_levels)
        self._sub_tree(topdir, levels, dirs_per_level)

        return

    def queue_walk_tree(self, path, tasks=2):
        '''import modules we wouldn't have normally used'''
        #import multiprocessing
        return

    def random_name(self, size=10, chars=string.ascii_lowercase + string.digits):
        '''return a random name'''
        rname = ''.join(random.choice(chars) for x in range(size))
        rname += self.suffix
        return rname


    def serial_create_dir_tree(self):
        '''Create a directory tree'''
        for d in self.dirs:
           if not os.path.exists(d):
               os.makedirs(d)
           
        return

    def serial_delete_dirs(self):
        '''Delete the FileTree root dir'''
        for d in self.dirs:
           if os.path.exists(d):
               shutil.rmtree(d)

        return

    def serial_populate_dir_tree(self):
        '''Write data files in serial to the directory tree'''
        for d in self.dirs:
            for f in range(self.files_per_dir):
                name = self.random_name()
                filename = os.path.join(d, name)
                result, err = self.write_file(filename)
                if not result:
                    print(err)
                    break

        return

    def walk_tree_generator(self, path):
        '''
        Returns a generator that can be used to walk a directory
        tree

        You can then make a list of all files via:
        files = []
        for dir in walk:
            for f in dir[2]:
                files.append("{0}/{1}".format(dir[0], f))

        Then use that for whatever...
        '''
        walk = os.walk(path)
        return walk

    def write_file(self, filename):       
        '''Create a number of random files in a directory tree of varying size'''
        # the number of bytes written is a multiple of the fs blocksize
        if self.fixed_size:
            num_bytes = self.max_size
        elif self.aligned and not self.fixed_size:
            num_bytes = random.randrange(self.bufsize, 
                    stop=self.max_size, step=self.bufsize)
        # pick a random bytesize between 0 and max_size
        else:
            num_bytes = random.randrange(1, self.max_size)

        # check to see if enough space is available
        if not self._free_space(num_bytes):
            return False, "Not enough space to write data."
        
        # check to see if path exists
        if not self._path_exists(filename):
            return False, "Directory does not exist."
        
        # figure out how many chunks we need to write
        bytes_left = num_bytes

        # write out the random data
        logging.debug("DEBUG: {0}.{1}(): Writing file: {2}".format(
                    self.__class__.__name__, self.write_file.__name__, filename))
        with open(filename, 'wb') as f:
            try:
                while bytes_left > 0: 
                    if bytes_left < self.bufsize:
                        f.write(os.urandom(bytes_left))
                        bytes_left -= self.bufsize
                    else:
                        f.write(os.urandom(self.bufsize))
                        bytes_left -= self.bufsize
            except IOError as ioe:
                print("IOError: {0}".format(ioe))
                print("We bail on IO Errors...")
                sys.exit(1)

        return True, "Success"

# for when you don't want to use the FileTree class, 
#   and simply want to create a random file
def create_random_file(name, numbytes):
    '''writes out a file full of random data'''
    path = os.path.dirname(os.path.abspath(name))
    vfsstats = os.statvfs(path)

    # dont write the file if there isn't enough free space on the filesystem
    if numbytes > (vfsstats.f_ffree * vfsstats.f_bfree):
        print("Not enough space to write data.")
        return

    bufsize = vfsstats.f_bsize

    if numbytes % bufsize != 0:
        print("Number of bytes must be a multiple of blocksize ({0})".format(
                bufsize))
        return

    bytes_left = numbytes

    with open(name, 'wb') as f:
        while bytes_left > 0: 
            f.write(os.urandom(bufsize))
            bytes_left -= bufsize
    return
