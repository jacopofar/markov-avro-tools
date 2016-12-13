from avro.datafile import DataFileReader
from avro.io import DatumReader
import time
import argparse
import operator
import json
# parameters from CLI
parser = argparse.ArgumentParser(description="builds a frequency table from a corpus file")
parser.add_argument("-input_file", type=str, help="the input avro file")
parser.add_argument("-tokenizer", type=str, help="the method to map text and tokens", default='split',
                    choices=['split', 'char', 'nltk'])
parser.add_argument("-case_insensitive", type=str, help="lowercase the tokens with the system locale", default='yes',
                    choices=['yes', 'no'])
parser.add_argument("-output_format", type=str, help="the output format for the frequency table", default=None,
                    choices=['tsv', 'jsons'])
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

input_file = args.input_file
output_format = args.output_format
case_insensitive = (args.case_insensitive == 'yes')
if output_format is None:
    if case_insensitive:
        print('the tokens will be lowercased using the system locale')
    else:
        print('the tokens will be counted as they are (case sensitive)')

    print('will build frequency list from file {0}'.format(input_file))

reader = DataFileReader(open(input_file, "rb"), DatumReader())
start_time = time.time()

token_count = {}

utterances_count = 0
max_tokens = 0
longest_description = ''
tags_counter = {}
total_length_chars = 0
total_length_tokens = 0

for utterance in reader:
    utterances_count += 1
    tokens = splitter(utterance['text'])
    total_length_tokens += len(tokens)
    for ct in tokens:
        if case_insensitive:
            t = ct.lower()
        else:
            t = ct
        if t in token_count.keys():
            token_count[t] += 1
        else:
            token_count[t] = 1

reader.close()

elapsed = time.time() - start_time
if output_format is None:
    print('processed {0} utterances containing {1} tokens, it took {2} seconds [{3} utterances per second]'
          .format(utterances_count, total_length_tokens, elapsed, utterances_count/elapsed))
    print('average tokens per sentence: {0}'.format(total_length_tokens/utterances_count))
    print('average characters per sentence: {0}'.format(total_length_chars/utterances_count))
    print('the longest utterance had {0} characters, it was:\n{1}\n'.format(max_tokens, longest_description))

max_rank = 5000
if output_format is None:
    print('{0} most common tags'.format(max_rank))
sorted_tokens = sorted(token_count.items(), key=operator.itemgetter(1), reverse=True)[:max_rank]

for k, v in sorted_tokens:
    if output_format is None:
        print('{0}: {1}'.format(k, v))
    if output_format == 'tsv':
        print('{0}\t{1}'.format(k, v))
    if output_format == 'jsons':
        print(json.dumps({'token': k, 'count': v}))

