"""
Functions for generating simple markov chains for sequences of words. Allows for scoring of
sentences based on completion frequency.
The original code is here: https://github.com/wieden-kennedy/python-markov
This is a rewrite to make it simpler, use UTF-8, run under Python 3 and use a while instead of recursion
"""
import random
import redis

PREFIX = 'markov'
SEPARATOR = '§'
STOP = '\x02'


class Markov(object):
    """
    Wrapper for markov functions
    """
    def __init__(self, prefix=None, key_length=2, completion_length=1, db=0, host='localhost', port=6379, password=None):
        self.client = redis.Redis(db=db, host=host, port=port, password=password)
        self.prefix = prefix or PREFIX
        self.key_length = key_length
        self.completion_length = completion_length

    def add_line_to_index(self, line):
        add_line_to_index(line, self.client, self.key_length, self.completion_length, self.prefix)

    def score_for_line(self, line):
        return score_for_line(line, self.client, self.key_length, self.completion_length, self.prefix)

    def generate(self, seed=None, max_words=1000):
        return generate(self.client, seed=seed, prefix=self.prefix, max_words=max_words, key_length=self.key_length)

    def flush(self, prefix=None):
        if prefix is not None:
            keys = self.client.keys("%s*" % self.prefix)
            for key in keys:
                self.client.delete(key)
    

def add_line_to_index(line, client, key_length=2, completion_length=1, prefix = PREFIX):
    """
    Add a line to our index of markov chains

    @param line: a list of words
    @param key_length: the desired length for our keys
    @param prefix: the prefix to use when saving the key on Redis
    @param client: Redis client
    @param completion_length: the desired completion length
    """
    key, completion = get_key_and_completion(line, key_length, completion_length, prefix)
    while key and completion:
        completion = make_key(completion)
        client.zincrby(key, completion)
        line = line[1:]
        key, completion = get_key_and_completion(line, key_length, completion_length, prefix)


def make_key(key, prefix=None):
    """
    Construct a Redis-friendly key from the list or tuple provided
    """
    if type(key) not in [str]:
        key = SEPARATOR.join(key)
        if prefix:
            key = SEPARATOR.join((prefix, key))
    return key


def max_for_key(key, client):
    """
    Get the maximum score for a completion on this key
    """
    maximum = client.zrevrange(key, 0, 0, withscores=True)
    if maximum:
        return maximum[0][1]
    else:
        return 0


def min_for_key(key, client):
    """
    Get the minimum score for a completion on this key
    """
    minimum = client.zrange(key, 0, 0, withscores=True)
    if minimum:
        return minimum[0][1]
    else:
        return 0


def score_for_completion(key, completion, client, normalize_to=100):
    """
    Get the normalized score for a completion
    """
    raw_score = client.zscore(key, make_key(completion)) or 0
    maximum = max_for_key(key, client) or 1
    return (raw_score/maximum) * normalize_to


def _score_for_line(line, client, key_length, completion_length, prefix, count=0):
    """
    Recursive function for iterating over all possible key/completion sets in a line
    and scoring them
    """
    score = 0
    key, completion = get_key_and_completion(line, key_length, completion_length, prefix)
    if key and completion:
        score = score_for_completion(key, completion, client)
        new_score, count = _score_for_line(line[1:], client, key_length, completion_length, prefix, count+1)
        score += new_score
    else:
        score = 0
    return score, count


def score_for_line(line, client, key_length=2, completion_length=1, prefix=PREFIX):
    """
    Score a line of text for fit based on our markov model
    """
    score, count = _score_for_line(line, client, key_length, completion_length, prefix)
    if count > 0:
        return score/count
    else:
        return 0


def generate(client, seed=None, prefix=None, max_words=1000, key_length=2):
    """
    Generate some text based on our model
    """
    if seed is None:
        key, seed = get_key_and_seed(client, prefix)
    partial = seed[:]
    #infinite while to avoid recursion which easily exceeded the stack
    while True:
        key = make_key(partial[-key_length:], prefix=prefix)
        completion = get_completion(client, key)
        if completion:
            completion = completion.decode('utf8').split(SEPARATOR)
            if count_tokens(partial) + count_tokens(completion) < max_words:
                partial += completion
            elif count_tokens(partial) + count_tokens(completion) == max_words:
                if STOP in completion:
                    completion.remove(STOP)
                return partial + completion
        else:
            if STOP in partial:
                partial.remove(STOP)
            return partial


def count_tokens(seed):
    """
    Count the tokens in the given seed.
    """
    return len(seed)


def get_key_and_seed(client, prefix=None, relevant_terms=None):
    """
    Wraps get_random_key_and_seed and get_relevant_key_and_seed
    """
    if relevant_terms and len(relevant_terms) > 0:
        return get_relevant_key_and_seed(client, relevant_terms, prefix)
    else:
        return get_random_key_and_seed(client, prefix)


def get_random_key_and_seed(client, prefix=None):
    """
    Get a random key from the data set and split it into a seed for sequence generation.
    """
    key = None
    seed = []
    while len(seed) == 0:
        if prefix:
            key = random.choice(client.keys("%s%s*" % (prefix, SEPARATOR)))
        else:
            key = client.randomkey()
        seed = key.decode('utf8').split(SEPARATOR)
    if prefix in seed:
        seed.remove(prefix) 
    return key, seed


def get_relevant_key_and_seed(client, relevant_terms, prefix=None, tries=10):
    """
    Get a key that contains one of the terms from relevant_terms.
    Limit the number of tries to avoid an infinite loop.
    """
    tried = 0
    key = None
    seed = []
    while len(seed) == 0 and tried < tries:
        keys = []
        for term in relevant_terms:
            if prefix:
                keys += client.keys("%s%s*%s*" % (prefix, SEPARATOR, term))
            else:
                keys += client.keys("*%s*" % term)
        try:
            key = random.choice(list(set(keys)))
            seed = key.split(SEPARATOR)
        except IndexError:
            # there were no matching keys
            break
        tried += 1
    if prefix in seed:
        seed.remove(prefix)
    return key, seed
        
    
def get_completion(client, key):
    """
    Get a possible completion for some key
    """
    completions = client.zrevrange(key, 0, -1, withscores=True)
    if len(completions) > 0:
        total = random.uniform(0, sum(w for c, w in completions))
        for candidate in completions:
            total -= candidate[1]
            if total <= 0:
                return candidate[0]
    return None


def get_key_and_completion(line, key_length, completion_length, prefix):
    """
    Get a key and completion from the given list of words
    """
    if len(line) >= key_length and STOP not in line[0:key_length]:
        key = make_key(line[0:key_length], prefix=prefix)
        if completion_length > 1:
            completion = line[key_length:key_length+completion_length]
        else:
            try:
                completion = line[key_length]
            except IndexError:
                completion = STOP
        completion = make_key(completion)
        return key, completion
    else:
        return False, False
