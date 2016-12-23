from avro.datafile import DataFileReader
from avro.io import DatumReader
import time
import argparse
import sqlite3

# parameters from CLI
parser = argparse.ArgumentParser(description="calculate a tag/token co-occurrency table from a corpus file")
parser.add_argument("-input_file", type=str, help="the input avro file")
parser.add_argument("-tokenizer", type=str, help="the method to map text and tokens", default='split',
                    choices=['split', 'char', 'nltk'])
parser.add_argument("-case_insensitive", type=str, help="lowercase the tokens with the system locale", default='yes',
                    choices=['yes', 'no'])
parser.add_argument('-sqlite_db_file', default='tag_token_counters.db', help='name of the SQLite file to create or add to')

args = parser.parse_args()

# pick the joiner and splitter functions, mapping plain text to token sequences
splitter = None
joiner = None

if args.tokenizer == 'char':

    def splitter(x): return list(x)

    def joiner(x): return ''.join(x)

if args.tokenizer == 'split':
    def splitter(x): return x.split(' ')

    def joiner(x): return ' '.join(x)


if args.tokenizer == 'nltk':
    from nltk.tokenize import RegexpTokenizer
    tokenizer = RegexpTokenizer('\w+|\$[\d\.]+|\S+')

    def splitter(x): return tokenizer.tokenize(x)


    def joiner(x): return ' '.join(x)


conn = sqlite3.connect(args.sqlite_db_file)
c = conn.cursor()
# table to count single tokens ignoring their tags
c.execute('CREATE TABLE token_count(token TEXT, count INTEGER)')
# table to count the token and tag co-occurrences. Note that an utterance can have 0, 1 or many tags
c.execute('CREATE TABLE token_tag_count(token TEXT, tag TEXT, count INTEGER)')

# dictionaries containing the data to be inserted/updated in small batches
pending_token_count = {}
pending_token_tag_count = {}
# how much pending data can we accept?
PENDING_MAX_SIZE = 3000


input_file = args.input_file
case_insensitive = (args.case_insensitive == 'yes')
if case_insensitive:
    print('the tokens will be lowercased using the system locale')
else:
    print('the tokens will be counted as they are (case sensitive)')


reader = DataFileReader(open(input_file, "rb"), DatumReader())
start_time = time.time()


utterances_count = 0
max_tokens = 0
longest_description = ''
tags_counter = {}
total_length_chars = 0
total_length_tokens = 0

for utterance in reader:
    utterances_count += 1
    tokens = splitter(utterance['text'])
    tags = utterance['tags']
    total_length_tokens += len(tokens)
    for ct in set(tokens):
        if case_insensitive:
            t = ct.lower()
        else:
            t = ct
        if t in pending_token_count:
            pending_token_count[t] += 1
        else:
            pending_token_count[t] = 1
        for tag in tags:
            if (tag, t) in pending_token_tag_count:
                pending_token_tag_count[(tag, t)] += 1
            else:
                pending_token_tag_count[(tag, t)] = 1
    if len(pending_token_count) + len(pending_token_tag_count) > PENDING_MAX_SIZE:
        print('writing on database, processed {0} utterances...'.format(utterances_count))
        tmp_pending = []
        for tag_token, count in pending_token_tag_count.items():
            tmp_pending.append({'tag': tag_token[0], 'token': tag_token[1], 'count': count})
        c.executemany('INSERT OR REPLACE INTO token_tag_count(token, tag, count) VALUES(:token, :tag, COALESCE((SELECT count + :count FROM  token_tag_count WHERE token = :token AND tag = :tag), :count))',
                  tmp_pending)
        conn.commit()
        tmp_pending = []

        for token, count in pending_token_count.items():
            tmp_pending.append({'token': token, 'count': count})
        c.executemany('INSERT OR REPLACE INTO token_count(token, count) VALUES(:token, COALESCE((SELECT count + :count FROM  token_count WHERE token = :token), :count))',
                      tmp_pending)
        conn.commit()
        print('wrote on database')
        pending_token_count = {}
        pending_token_tag_count = {}


reader.close()

elapsed = time.time() - start_time
print('processed {0} utterances, it took {1} seconds [{2} utterances per second]'.format(utterances_count, elapsed, utterances_count/elapsed))