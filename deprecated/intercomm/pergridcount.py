import argparse

from mpi4py import MPI
from utils import ConciseTweet  # utils.py

# Command-line arguments

parser = argparse.ArgumentParser(description='Per-grid counter')
parser.add_argument('twitter_path', type=str,
                    help='Path to the twitter data file')
args = parser.parse_args()

GRID_CELLNAME = ['A1', 'B1', 'C1', 'D1', 'A2', 'B2', 'C2', 'D2', 
                 'A3', 'B3', 'C3', 'D3', 'A4', 'B4', 'C4', 'D4']


comm = MPI.Comm.Get_parent()   # count.py
size = comm.Get_size()
rank = comm.Get_rank()         # 0 ~ 15

grid = None
comm.bcast(grid, root=MPI.ROOT)

with open(args.twitter_path, 'r') as ft:
    for (i, line) in enumerate(ft):
        # round-robin job allocation to each process
        if i % size == rank:
            c_tweet = ConciseTweet(line)
            # not null
            if c_tweet.coord and c_tweet.lang:

                print (rank, GRID_CELLNAME[rank], ':\n',
                       c_tweet.coord, c_tweet.lang, '\n',
                       grid, '\n')

comm.Disconnect()
