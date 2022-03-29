import argparse
import json

from mpi4py import MPI
from collections import Counter, defaultdict

from utils import addDictCounter, count, output

# Command-line arguments

parser = \
    argparse.ArgumentParser(description='Twitter Language Geospatial Analysis for Sydney'
                            )
parser.add_argument('twitter_path', type=str,
                    help='Path to the twitter data file'
                    )
parser.add_argument('grid_path', type=str,
                    help='Path to the grid shape file'
                    )
parser.add_argument('--batch_size', type=int, default=50,
                    help='The number of tweets bared per message'
                    )
args = parser.parse_args()


def main():

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    cell_stats = defaultdict(Counter)
    
    with open(args.grid_path, 'r') as fg:
        grids = [(f['geometry']['coordinates'][0])[:4] for f in
                 json.load(fg)['features']]

    # MPI not needed

    if size == 1:
        with open(args.twitter_path, 'rb') as ft:
            cell_stats = count(grids, ft, cell_stats)

    # share the work

    else:
        with open(args.twitter_path, 'rb') as ft:   # as bytes
            # specify read range
            ft.seek(0, 2)
            portion_len = ft.tell() // size
            read_start, read_end = map((portion_len).__mul__, (rank, rank+1))

            # ensure end before EOF
            if rank == size - 1:
                read_end = ft.tell() - 1

            # ditch partial line
            ft.seek(read_start)
            if rank != 0:
                read_start += len(ft.readline())

            batch = []
            while read_start <= read_end:
                line = ft.readline()
                read_start += len(line)
                batch.append(line)
                if len(batch) >= args.batch_size or read_start > read_end:
                    cell_stats = count(grids, batch, cell_stats)
                    batch.clear()

        # reducing
        cell_stats = comm.reduce(
            cell_stats, op=MPI.Op.Create(addDictCounter, commute=True), root=0
        )


    if rank == 0:
        output(cell_stats)


if __name__ == '__main__':
    main()

