import argparse
import json

from mpi4py import MPI
from collections import Counter, defaultdict
from utils import addDictset, count, output   # utils.py

# Command-line arguments

parser = \
    argparse.ArgumentParser(description='Twitter Language Geospatial Analysis for Sydney'
                            )
parser.add_argument('--twitter_path', type=str,
                    default=r'data/largeTwitter.json',
                    help='Path to the twitter data file')
parser.add_argument('--out_path', type=str, default=r'out/results.txt',
                    help='Path to output')
parser.add_argument('--batch_size_per_message', type=int, default=50,
                    help='The number of tweets bared per message'
                    )
args = parser.parse_args()

GRID_PATH = r'data/sydGrid.json'   # path to the grid shape file
N_GRID = 16   # number of grids


def main():

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    cell_lang_cnt = defaultdict(set)
    cell_tweet_cnt, lang_tweet_cnt = Counter(), Counter()
    
    with open(GRID_PATH, 'r') as fg:
        grids = [(f['geometry']['coordinates'][0])[:4] for f in
                 json.load(fg)['features']]

    # MPI not needed

    if size == 1:

        pass   # @TODO

    # rank-0 task distributor

    else:
        if rank == 0:
            with open(args.twitter_path, 'r') as ft:
                to = 1
                batch = []
                for i, line in enumerate(ft):
                    batch.append(line)
                    if (i + 1) % args.batch_size_per_message == 0:   # full batch
                        comm.send(batch, dest=to, tag=31)
                        to = (to + 1 if to < size - 1 else 1)
                        batch.clear()
                # last batch of tweets
                if batch:
                    comm.send(batch, dest=to, tag=31)
                # stop send
                for i in range(1, size):
                    comm.send([], dest=i, tag=31)

        # other workers

        else:
            while True:
                batch = comm.recv(source=0, tag=31)
                if not batch:   # []
                    break
                else:
                    (cell_lang_cnt, cell_tweet_cnt, lang_tweet_cnt) = \
                        count(
                            grids,
                            batch,
                            cell_lang_cnt,
                            cell_tweet_cnt,
                            lang_tweet_cnt,
                            )

        # gathering

        dictsetSumOp = MPI.Op.Create(addDictset, commute=True)
        counterSumOp = MPI.Op.Create(lambda c1, c2, datatype: c1 + c2,
                                     commute=True)
        cell_lang_cnt  = comm.reduce(cell_lang_cnt,  op=dictsetSumOp, root=0)
        cell_tweet_cnt = comm.reduce(cell_tweet_cnt, op=counterSumOp, root=0)
        lang_tweet_cnt = comm.reduce(lang_tweet_cnt, op=counterSumOp, root=0)

    output(cell_lang_cnt, cell_tweet_cnt, lang_tweet_cnt,
           args.out_path)


if __name__ == '__main__':
    main()
