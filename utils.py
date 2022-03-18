import json

from collections import Counter

LANG_ATR = {
    'en': 'English',
    'ar': 'Arabic',
    'bn': 'Bengali',
    'cs': 'Czech',
    'da': 'Danish',
    'de': 'German',
    'el': 'Greek',
    'es': 'Spanish',
    'fa': 'Persian',
    'fi': 'Finnish',
    'fil': 'Filipino',
    'fr': 'French',
    'he': 'Hebrew',
    'hi': 'Hindi',
    'hu': 'Hungarian',
    'id': 'Indonesian',
    'it': 'Italian',
    'ja': 'Japanese',
    'ko': 'Korean',
    'msa': 'Malay',
    'nl': 'Dutch',
    'no': 'Norwegian',
    'pl': 'Polish',
    'pt': 'Portuguese',
    'ro': 'Romanian',
    'ru': 'Russian',
    'sv': 'Swedish',
    'th': 'Thai',
    'tr': 'Turkish',
    'uk': 'Ukrainian',
    'ur': 'Urdu',
    'vi': 'Vietnamese',
    'zh-cn': 'Chinese',
    'zh-tw': 'Chinese',
    }

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
    Only handles grids of size 4 x 4

    :return: cell that coord locates in, return None if found outside the grids
    :rtype: str

    """
    for i, cell in enumerate(grids):
        llong, rlong, ulat, dlat = (cell[0][0], cell[1][0],
                cell[0][1], cell[2][1])
        h = llong < coord[0] and coord[0] <= rlong
        v = ulat >= coord[1] and coord[1] > dlat
        if h and v or \
                i in [0, 1, 2] and llong == coord[0] or \
                i in [7, 11, 15] and coord[1] == dlat or \
                i == 3 and coord == cell[3]:
            return 'ABCD'[i % 4] + str(i // 4 + 1)
    return None


def addDictset(d1, d2, datatype):
    """
    :type d1, d2: defaultdict(set)
    :return: merged new object

    """
    for k, v in d2.items():
        d1[k].update(v)
    return d1


def output(cell_lang_cnt, cell_tweet_cnt, lang_tweet_cnt):

    print('''
===================================================
''')
    print('Cell\t#Total Tweets\t#Number of Languages Used\n')
    for cell in ['A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4',
                 'C1', 'C2', 'C3', 'C4', 'D1', 'D2', 'D3', 'D4']:
        print(cell, '\t', end='')
        print(cell_tweet_cnt[cell], '\t\t', end='')
        print(len(cell_lang_cnt[cell]))
    print('''
===================================================
''')
    print('#Top 10 Languages & #Tweets')
    # merge chinese
    lang_tweet_cnt['zh-cn'] += lang_tweet_cnt['zh-tw']
    del lang_tweet_cnt['zh-tw']
    top = Counter({
        LANG_ATR[l]: c
        for l, c in lang_tweet_cnt.items() if c > 0 and l in LANG_ATR
    }).most_common(10)
    for l, c in top:
        print('\n' + l, '\t', c)

    print('''
===================================================''')
    return
