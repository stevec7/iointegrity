#!/usr/bin/env python
import logging
import os
import sys
from mpi4py import MPI
from optparse import OptionParser
from iointegrity.iotools import create_random_file

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
    filename = "{0}/{1}{2}.dat"

    # create a list of files to pass to the ranks
    files = [ filename.format(options.dir, options.prefix, n)
                for n in range(0, options.numfiles) ]

    # if num_ranks is 1, then we exit...
    if num_ranks == 1:
        print "Need more than 1 rank Ted..."
        comm.Abort(1)

    for f in files:
        # wait for another rank to report in
        child = comm.recv(source=MPI.ANY_SOURCE)

        logging.verbose("Rank {0} reported in, sending: {1}".format(child, f))

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
    rank = comm.Get_rank()
    done = False

    while not done:
        comm.send(rank, dest=0)
        filename = comm.recv(source=0)

        if filename == 'alldone':
            done = True
        else:
            create_random_file(filename, options.size)
            done = False
    return

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-d', '--dir',
                    dest='dir',
                    type='str',
                    help='full path to dir where files will be created.')
    parser.add_option('-n', '--numfiles',
                    dest='numfiles',
                    type='int',
                    help='number of files to create')
    parser.add_option('-p', '--prefix',
                    dest='prefix',
                    type='str',
                    help='prefix to add to the beginning of the files')
    parser.add_option('-s', '--size',
                    dest='size',
                    type='int',
                    help='file size in bytes')
    parser.add_option('-v', '--verbose',
                    action='store_true',
                    dest='verbose',
                    help='show verbose output')
    options, args = parser.parse_args()
    main(options, args)
