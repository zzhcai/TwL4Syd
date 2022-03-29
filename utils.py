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


def count(grids, batch, cell_stats):
    """
    Accumulators fold by new batch of tweets

    :param grids: coordinates of grid cells
    :param batch: a list of byte-format tweet strings
    :param cell_stats: { cell: {lang: #tweets} }
    :type  cell_stats: defaultdict(Counter)
    :return: updated stats

    """
    for line in batch:
        c_tweet = ConciseTweet(line.decode('utf-8'))
        if c_tweet.coord and c_tweet.lang:
            cell = locate(c_tweet.coord, grids)
            if cell != None:   # inside mesh
                cell_stats[cell][c_tweet.lang] += 1

    return cell_stats


def locate(coord, grids):
    """
    Only handles grids of size 4 x 4
    The region codes are hard coded for sydGrid.json.

    :return: cell that coord locates in, return None if found outside the grids
    :rtype: str

    """
    for i, cell in enumerate(grids):
        llong, rlong, ulat, dlat = (cell[0][0], cell[2][0],
                cell[0][1], cell[1][1])
        h = llong < coord[0] and coord[0] <= rlong
        v = ulat >= coord[1] and coord[1] > dlat
        if h and v or \
                i in [11, 12, 13, 14] and llong == coord[0] and v or \
                i in [15, 3, 7, 11] and coord[1] == dlat and h or \
                i == 11 and coord == cell[1]:
            return ['C4', 'B4', 'A4', 'D3', 'C3', 'B3', 'A3', 
                    'D2', 'C2', 'B2', 'A2', 'D1', 'C1', 'B1', 'A1', 'D4'][i]
    return None


def addDictCounter(d1, d2, datatype):
    """
    :type d1, d2: defaultdict(Counter)

    """
    for k, v in d2.items():
        d1[k].update(v)
    return d1


def output(cell_stats):

    print('''
===================================================
''')

    print('Cell\t#Total Tweets\t#Number of Languages Used\t#Top 10 Languages & #Tweets\n')
    for cell in ['A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4',
                 'C1', 'C2', 'C3', 'C4', 'D1', 'D2', 'D3', 'D4']:

        del cell_stats[cell]['und']
        cell_stats[cell]['zh-cn'] += cell_stats[cell]['zh-tw']
        del cell_stats[cell]['zh-tw']
        cell_stats[cell]['id'] += cell_stats[cell]['in']
        del cell_stats[cell]['in']
        cell_stats[cell] = Counter({l: c for l, c in cell_stats[cell].items() if c > 0})

        print(cell, '\t', end='')
        print(sum(cell_stats[cell].values()), '\t\t', end='')
        print(len(cell_stats[cell]), '\t\t\t\t', end='')
        print(tuple(['{0}-{1}'.format((LANG_ATR[l] if l in LANG_ATR else l), c)
            for l, c in cell_stats[cell].most_common(10)]))

    print('''
===================================================''')
    return

