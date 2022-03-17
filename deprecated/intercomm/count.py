import argparse
import sys
import json

from mpi4py import MPI

# Command-line arguments

parser = \
    argparse.ArgumentParser(description='Twitter Language Geospatial Analysis for Sydney'
                            )
parser.add_argument('--grid_path', type=str,
                    default=r'data/sydGrid.json',
                    help='Path to the grid shape file')
parser.add_argument('--twitter_path', type=str,
                    default=r'data/largeTwitter.json',
                    help='Path to the twitter data file')
args = parser.parse_args()

N_GRID = 16


def main():

    # extract grid shapes
    with open(args.grid_path, 'r') as fg:
        try:
            grids = [(grid['geometry']['coordinates'][0])[:4]
                     for grid in json.load(fg)['features']]
        except json.decoder.JSONDecodeError:
            sys.exit(0)

    # create one worker for each grid
    comm = MPI.COMM_SELF.Spawn(sys.executable, args=['pergridcount.py',
                               args.twitter_path], maxprocs=N_GRID)
    # broadcast grid shapes
    comm.bcast(grids, root=MPI.ROOT)

    comm.Disconnect()


if __name__ == '__main__':
    main()
