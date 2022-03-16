import argparse
from mpi4py import MPI

from utils import ConciseTweet

# Command-line arguments
parser = argparse.ArgumentParser(description='Twitter Geospatial Language Analysis for Sydney')
parser.add_argument('--grid_path', type=str, default=r'data/sydGrid.json',
                    help='Path to the grid shape file')
parser.add_argument('--twitter_path', type=str, default=r'data/largeTwitter.json',
                    help='Path to the twitter data file')
args = parser.parse_args()

def main():

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    with open(args.twitter_path, 'r') as fp:
        for i, line in enumerate(fp):
            # round-robin job allocation to each process
            if i % size == rank:
                c_tweet = ConciseTweet(line)
                if c_tweet.coord and c_tweet.lang:   # not null
                    pass

if __name__ == '__main__':
    main()
