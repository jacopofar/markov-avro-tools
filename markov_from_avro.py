from markov import Markov

from avro.datafile import DataFileReader
from avro.io import DatumReader
import time
import argparse


# parameters from CLI
parser = argparse.ArgumentParser(description="build and store a Markov model in Redis, use it to generate text")
parser.add_argument("operation", type=str, help="what to do", default="build", choices=['build', 'generate'])
parser.add_argument("-keylen", type=int, help="the N-gram size", default=3)
parser.add_argument("-input_file", type=str, help="the input avro file", default="utterances.avro")
parser.add_argument("-skip_to", type=int, help="ignore the first N utterances (used to resume importing)", default=0)
parser.add_argument("-tokenizer", type=str, help="the method to map text and tokens", default='split',
                    choices=['split', 'char', 'nltk'])
parser.add_argument("-markov_prefix", type=str, help="the prefix for redis keys", default="mkv")
parser.add_argument("-tags", type=str, help="the tags, if any, to be filtered in the corpus", default="")
parser.add_argument("-number", type=int, help="the number of utterances to generate", default=1)
parser.add_argument("-max_length", type=int, help="the maximum numbers of token per utterences to generate", default=1000)
parser.add_argument("-seed", type=str, help="the seed generate utterances")

args = parser.parse_args()

keylen = args.keylen
prefix = args.markov_prefix

start_seq = list('°' * keylen)


# pick the joiner and splitter functions, mapping plain text to sequences markov states labels
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

mm = Markov(key_length=keylen, prefix=prefix)

if args.operation == 'build':
    tags = list(filter(lambda x: x != '',  args.tags.split(',')))
    input_file = args.input_file
    skip_to = args.skip_to
    if len(tags) > 0:
        print('will import file {0} using key prefix {1}, filtering tags {2}'.format(input_file, prefix, tags))
    else:
        print('will import all utterances in file {0} using key prefix {1}'.format(input_file, prefix))
    reader = DataFileReader(open(input_file, "rb"), DatumReader())
    # counters to log performances
    start_time = time.time()
    utterances_count = 0
    tokens_count = 0

    for utterance in reader:
        utterances_count += 1
        if utterances_count < skip_to:
            continue
        if len(tags) > 0:
            # the utterance has no tags, ignore it
            if utterance['tags'] is None:
                continue
            # check that at least one of the required tags is present
            if not any(filter(lambda t: t in tags, utterance['tags'])):
                continue
        tokens = start_seq + splitter(utterance['text'])
        # print('processing {0} tokens in utterance {1}'.format(len(tokens), utterances_count))
        tokens_count += len(tokens)
        mm.add_line_to_index(tokens)
        if utterances_count % 1000 == 0:
            elapsed = time.time() - start_time
            print(' --- so far, processed {0} utterances containing {1} tokens, it took {2} seconds [{3} utterances per second]'
                  .format(utterances_count, tokens_count, elapsed, utterances_count / elapsed))

    elapsed = time.time() - start_time
    print('processed {0} utterances containing {1} tokens, it took {2} seconds [{3} utterances per second]'
          .format(utterances_count, tokens_count, elapsed, utterances_count/elapsed))
    reader.close()

if args.operation == 'generate':
    seed = start_seq
    if args.seed is not None:
        # if the seed is shorter than keylen, pad it with the starting sequence
        seed = start_seq + splitter(args.seed)
        seed = seed[:-keylen]
    print('using seed {0}'.format(seed))
    for i in range(args.number):
        # use the starting sequence but remove it from the result
        gen = mm.generate(seed=seed, max_words=args.max_length)[keylen:]
        print(joiner(gen))

# TODO add likelihood calculation
