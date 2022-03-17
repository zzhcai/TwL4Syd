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
        :return: a formatted Python object if is a tweet
        :rtype: dict
        
        """

        if line.startswith('{"id"'):
            if line.endswith(',\n'):
                line = line[:-2]
            elif line.endswith(']}\n'):
                line = line[:-3]
            try:
                return json.loads(line)
            except json.decoder.JSONDecodeError:
                pass
        return None
