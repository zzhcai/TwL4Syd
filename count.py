import argparse
import json

from mpi4py import MPI
from collections import Counter, defaultdict

from utils import addDictset, count, output   # utils.py

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
parser.add_argument('--batch_size_per_message', type=int, default=50,
                    help='The number of tweets bared per message'
                    )
args = parser.parse_args()

N_GRID = 16   # number of grids


def main():

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    cell_lang_dict = defaultdict(set)
    cell_tweet_cnt, lang_tweet_cnt = Counter(), Counter()
    
    with open(args.grid_path, 'r') as fg:
        grids = [(f['geometry']['coordinates'][0])[:4] for f in
                 json.load(fg)['features']]

    # MPI not needed

    if size == 1:
        with open(args.twitter_path, 'r') as ft:
            (cell_lang_dict, cell_tweet_cnt, lang_tweet_cnt) = \
                count(
                    grids,
                    ft,
                    cell_lang_dict,
                    cell_tweet_cnt,
                    lang_tweet_cnt,
                    )

    # share the work

    else:
        with open(args.twitter_path, 'r') as ft:
            batch = []
            for i, line in enumerate(ft):
                if i % size == rank:
                    batch.append(line)
                    if len(batch) >= args.batch_size_per_message:   # full batch
                        (cell_lang_dict, cell_tweet_cnt, lang_tweet_cnt) = \
                            count(
                                grids,
                                batch,
                                cell_lang_dict,
                                cell_tweet_cnt,
                                lang_tweet_cnt,
                                )
                        batch.clear()
            # last batch of tweets
            if batch:
                (cell_lang_dict, cell_tweet_cnt, lang_tweet_cnt) = \
                    count(
                        grids,
                        batch,
                        cell_lang_dict,
                        cell_tweet_cnt,
                        lang_tweet_cnt,
                        )

        # gathering

        dictsetSumOp = MPI.Op.Create(addDictset, commute=True)
        counterSumOp = MPI.Op.Create(lambda c1, c2, datatype: c1 + c2,
                                     commute=True)
        cell_lang_dict = comm.reduce(cell_lang_dict, op=dictsetSumOp, root=0)
        cell_tweet_cnt = comm.reduce(cell_tweet_cnt, op=counterSumOp, root=0)
        lang_tweet_cnt = comm.reduce(lang_tweet_cnt, op=counterSumOp, root=0)


    if rank == 0:
        output(cell_lang_dict, cell_tweet_cnt, lang_tweet_cnt)


if __name__ == '__main__':
    main()
