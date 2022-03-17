import json


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


# @TODO: Remove rank
def count(rank, grids, batch, cell_tweetlang_dict, lang_tweet_dict):
    """
    Accumulator folds by new batch of tweets
    
    :param grids: coordinates of grid cells
    :param batch: a list of str tweets
    :param cell_tweetlang_dict: {cell: (#tweets, #lang)}
    :param lang_tweet_dict: {lang: #tweets}
    :return: 2 updated dictionaries
    
    """
    for line in batch:
        c_tweet = ConciseTweet(line)
        if c_tweet.coord and c_tweet.lang:
            # @TODO
            print(rank, c_tweet.coord, c_tweet.lang)
    
    return cell_tweetlang_dict, lang_tweet_dict

def output():
    """
    
    
    """


def output_mpi(cell_tweetlang_dict, lang_tweet_dict):
    """
    
    
    """
