#!/usr/bin/env python
import datetime
import logging
import os
import sys
from mpi4py import MPI
from optparse import OptionParser
from iointegrity.iotools import create_random_file, FileMD5
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

    return

def create(options, work):
    fmd5 = FileMD5()
    results = []

    for w in work:
        create_random_file(w, options.size)
        md5sum = fmd5.create_md5(w)
        results.append((w, md5sum))

    return results

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
        print("Need more than 1 rank Ted...")
        comm.Abort(1)

	# split into chunks since mpi4py's scatter cannot take a size arg
    chunks = [[] for _ in range(comm.Get_size())]
    for e, chunk in enumerate(files):
        chunks[e % comm.Get_size()].append(chunk)

    rc = comm.scatter(chunks)
    results = create(options, rc)

    # get results and add to a database
    results = comm.gather(results, root=0)
    db = IOIntegrityDB(options.dbfile)

    mydate = datetime.datetime.now().isoformat()
    to_insert = []
    for rank, r in enumerate(results):
        logging.debug("Rank: {}, Results: {}".format(rank, r))
        for i in r:
            to_insert.append((i[0], i[1], '', '', mydate))

    db.mass_insert_filemd5(to_insert)
        

    return

def slave(comm, options):
    rank = comm.Get_rank()

    data = []
    results = []
    data = comm.scatter(data, root=0)

    start_time = MPI.Wtime()
    results = create(options, data)
    elapsed = MPI.Wtime() - start_time

    # these will be committed to a database
    results = comm.gather(results, root=0)
    
    return

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-d', '--dir',
                    dest='dir',
                    type='str',
                    help='full path to dir where files will be created.')
    parser.add_option('-f', '--dbfile',
                    dest='dbfile',
                    type='str',
                    help='full path to the database file.')
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
