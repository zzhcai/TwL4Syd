import json

from copy import deepcopy


class ConciseTweet:

    """
    Concise tweet information,
    only containing location coordinates, and language code

    """

    def __init__(self, line):
        """
        :param line: a line in the twitter data file
        :type line: JSON String document

        """

        self.coord = self.lang = None

        tweet = self.truncated(line)
        if tweet and 'doc' in tweet:
            doc = tweet['doc']
            if 'coordinates' in doc and doc['coordinates'] \
                and 'coordinates' in doc['coordinates']:
                self.coord = doc['coordinates']['coordinates']
            elif 'geo' in doc and doc['geo'] and 'coordinates' \
                in doc['geo']:
                self.coord = doc['geo']['coordinates'][::-1]
            self.lang = (doc['lang'] if 'lang' in doc
                         and isinstance(doc['lang'], str) else None)

    @staticmethod
    def truncated(line):
        """
        :param line: a line in the twitter data file
        :type line: JSON String document
        :return: a formatted Python object if is a tweet, otherwise None
        :rtype: dict

        """

        if line.startswith('{"id"'):
            line = line.strip()
            if line.endswith(','):
                line = line[:-1]
            elif line.endswith(']}'):
                line = line[:-2]
            try:
                return json.loads(line)
            except json.decoder.JSONDecodeError:
                pass
        return None


def count(grids, batch, cell_lang_cnt, cell_tweet_cnt, lang_tweet_cnt):
    """
    Accumulators fold by new batch of tweets

    :param grids: coordinates of grid cells
    :param batch: a list of str tweets
    :param cell_lang_cut: {cell: {lang}}
    :param cell_tweet_cnt: {cell: #tweets}
    :param lang_tweet_cnt: {lang: #tweets}
    :return: 1 updated dict, 2 updated counters in tuple

    """
    for line in batch:
        c_tweet = ConciseTweet(line)
        if c_tweet.coord and c_tweet.lang:
            cell = locate(c_tweet.coord, grids)
            # inside
            if cell != None:
                cell_tweet_cnt[cell] += 1
                lang_tweet_cnt[c_tweet.lang] += 1
                cell_lang_cnt[cell].add(c_tweet.lang)

    return cell_lang_cnt, cell_tweet_cnt, lang_tweet_cnt


def locate(coord, grids):
    """
    :return: cell that coord locates in, return None if found outside the grids
    :rtype: str

    """
    for cell in grids:
        


def addDictset(d1, d2):
    """
    :type d1, d2: defaultdict(set)
    :return: merged new object

    """
    for k, v in d2.items():
        d1[k].update(v)
    return d1


def output(cell_lang_cnt, cell_tweet_cnt, lang_tweet_cnt, out_path):

    print(cell_lang_cnt, cell_tweet_cnt, lang_tweet_cnt.most_common(10))
    return
