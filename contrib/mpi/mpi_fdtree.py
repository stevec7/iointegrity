#!/usr/bin/env python
import logging
import os
import sys
from mpi4py import MPI
from optparse import OptionParser
from iointegrity.iotools import FileTree

def main(options, args):
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    # setup logging
    if options.verbose:
        logging.basicConfig(format='%(message)s', level=logging.DEBUG)

    if rank == 0:
        # do the master
        master(comm, options)
    else:
        # secondary stuff
        slave(comm, options)

def master(comm, options):
    '''rank 0 will handle the program setup, and distribute
       tasks to all of the other ranks'''
    num_ranks = comm.Get_size()

    # initialize the FileTree class
    ft = FileTree()
    ft.set_config(vars(options))

    # create an array of directories first
    ft.gen_dir_array(ft.topdir, ft.num_levels, ft.dirs_per_level)    

    # make the directories in serial for now (booooo!)
    ft.serial_create_dir_tree()

    # create a file list
    files = [ os.path.join(d, ft.random_name()) 
        for n in range(ft.files_per_dir) 
        for d in ft.dirs ]
    #from IPython import embed; embed()

    # if num_ranks is 1, then we exit...
    if num_ranks == 1:
        print("Need more than 1 rank Ted...")
        comm.Abort(1)

    for f in files:
        # wait for another rank to report in
        child = comm.recv(source=MPI.ANY_SOURCE)

        logging.debug("DEBUG: Rank {0} reported in, sending: {1}".format(child, f))

        # send filename to this rank
        if child:
            comm.send(f, dest=child)

    # ran out of files to create. tell ranks we're done
    i = 1
    while i < num_ranks:
        child = comm.recv(source=MPI.ANY_SOURCE)
        comm.send('alldone', dest=child)
        i += 1
        
    return

def slave(comm, options):
    # initialize the FileTree class
    ft = FileTree()
    ft.set_config(vars(options))

    rank = comm.Get_rank()
    done = False

    while not done:
        comm.send(rank, dest=0)
        filename = comm.recv(source=0)

        if filename == 'alldone':
            done = True
        else:
            ft.write_file(filename)
            done = False
    return

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-a', '--aligned',
                    action='store_true',
                    dest='aligned',
                    help='if using random file size (--random-size)'\
                    ', align the size to the filesystem blocksize')
    parser.add_option('--fixed-size',
                    action='store_true',
                    dest='fixed_size',
                    help='use a fixed file size equal to (--max-size).')
    parser.add_option('--num-dirs-per-level',
                    dest='dirs_per_level',
                    type='int',
                    help='number of directories per level')
    parser.add_option('--num-files',
                    dest='files_per_dir',
                    type='int',
                    help='number of files per directory')
    parser.add_option('--num-levels',
                    dest='num_levels',
                    type='int',
                    help='number of directory levels')
    parser.add_option('--max-size',
                    dest='max_size',
                    type='int',
                    help='max file size in bytes')
    parser.add_option('--random-size',
                    action='store_true',
                    dest='random_size',
                    help='randomize the file size')
    parser.add_option('--suffix',
                    dest='suffix',
                    type='str',
                    help='suffix to add to random filename.')
    parser.add_option('--top-dir',
                    dest='topdir',
                    type='str',
                    help='full path to dir where files will be created.')
    parser.add_option('-v', '--verbose',
                    action='store_true',
                    dest='verbose',
                    help='show verbose output')
    options, args = parser.parse_args()
    main(options, args)
