#!/usr/bin/env python
import logging
import marshal
import os
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
    else:
        logging.basicConfig(format='%(message)s', level=logging.INFO)

    if rank == 0:
        # do the master
        master(comm, options)
    else:
        # secondary stuff
        slave(comm, options)

    MPI.Finalize()
    return

def master(comm, options):
    '''rank 0 will handle the program setup, and distribute
       tasks to all of the other ranks'''
    num_ranks = comm.Get_size()

    iodb = IOIntegrityDB(options.db)    
    data = iodb.get_all_file_info()

    bad_blocks = []

    # if num_ranks is 1, then we exit...
    if num_ranks == 1:
        print("Need more than 1 rank Ted...")
        comm.Abort(1)

    # split into chunks since mpi4py's scatter cannot take a size arg
    chunks = [[] for _ in range(comm.Get_size())]
    for e, chunk in enumerate(data):
        chunks[e % comm.Get_size()].append(dict(chunk))

    rc = comm.scatter(chunks)
    results = {}
    results = comm.gather(results, root=0)

    for rank, r in enumerate(results):
        logging.info("Rank: {}, Results: {}".format(rank, r))
    return

def slave(comm, options):
    rank = comm.Get_rank()
    done = False

    # setup the objects
    fmd5 = FileMD5()
    blk = BlockMD5()

    data = {}
    results = {}
    data = comm.scatter(data, root=0)

    for d in data:
        is_clean = fmd5.validate_md5(d['name'], d['md5sum'])

        if is_clean is not True:
            logging.debug("Rank: {0}, Bad_blocks: {1}".format(
                        rank, d['name']))
            mapd = marshal.loads(d['chunks'])
            block_check = blk.validate_map(d['name'], mapd)
            if block_check is not True:
                #for b in block_check[1]:
                #    print d['name'], b 
                #comm.send(block_check, dest=0)
                results[d['name']] = block_check[1]

    results = comm.gather(results, root=0)
    return

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-d', '--db',
                    dest='db',
                    type='str',
                    help='database file to open.')
    parser.add_option('-v', '--verbose',
                    action='store_true',
                    dest='verbose',
                    help='show verbose output')
    options, args = parser.parse_args()
    main(options, args)
