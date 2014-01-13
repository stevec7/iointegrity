#!/usr/bin/env python
import logging
import os
import pickle
import sys
from mpi4py import MPI
from optparse import OptionParser
from iointegrity.iotools import BlockMD5, FileMD5
from iointegrity.dbtools import IOIntegrityDB

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

    iodb = IOIntegrityDB(options.db)    
    data = iodb.get_all_file_info()

    bad_blocks = []

    # if num_ranks is 1, then we exit...
    if num_ranks == 1:
        print "Need more than 1 rank Ted..."
        comm.Abort(1)

    for d in data:
        # wait for another rank to report in
        child = comm.recv(source=MPI.ANY_SOURCE)

        logging.debug("Master: Received: {0}".format(child))

        # send file data to this rank
        if child:
            logging.debug("Sending '{0}' to rank: '{1}'".format(
                    d['name'], child))
            # tag=2 means we are sending more data
            comm.send(dict(d), dest=child, tag=2)

    # ran out of files to create. tell ranks we're done
    i = 1
    while i < num_ranks:
        child = comm.recv(source=MPI.ANY_SOURCE)
        logging.debug("Master: Received: {0}".format(child))
        comm.send('alldone', dest=child)
        i += 1
        
    return

def slave(comm, options):
    rank = comm.Get_rank()
    done = False

    # setup the objects
    fmd5 = FileMD5()
    blk = BlockMD5()

    while not done:
        comm.send(rank, dest=0)
        d = comm.recv(source=0, tag=2)

        logging.debug("Rank: {0}, received: {1}".format(rank, d))

        if d == 'alldone':
            done = True
        else:
            is_clean = fmd5.validate_md5(d['name'], d['md5sum'])

            if is_clean is not True:
                logging.debug("Rank: {0}, Bad_blocks: {1}".format(
                            rank, d['name']))
                map = pickle.marshal.loads(d['chunks'])
                block_check = blk.validate_map(d['name'], map)
                if block_check is not True:
                    #for b in block_check[1]:
                    #    print d['name'], b 
                    #comm.send(block_check, dest=0)
                    buf = ( d['name'], block_check )
                    comm.send(buf, dest=0, tag=1)
            done = False
    return

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-d', '--db',
                    dest='db',
                    type='str',
                    help='database file to openi.')
    parser.add_option('-v', '--verbose',
                    action='store_true',
                    dest='verbose',
                    help='show verbose output')
    options, args = parser.parse_args()
    main(options, args)
