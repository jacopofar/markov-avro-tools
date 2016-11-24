from avro.datafile import DataFileReader
from avro.io import DatumReader
import time
import argparse
import operator

# parameters from CLI
parser = argparse.ArgumentParser(description="shows statistics about a corpus file")
parser.add_argument("-input_file", type=str, help="the input avro file", default="utterances.avro")
parser.add_argument("-tokenizer", type=str, help="the method to map text and tokens", default='split',
                    choices=['split', 'char'])

args = parser.parse_args()

# pick the joiner and splitter functions, mapping plain text to sequences markov states labels
splitter = None
joiner = None

if args.tokenizer == 'char':

    def splitter(x): return list(x)

    def joiner(x): return ''.join(x)

if args.tokenizer == 'split':
    def splitter(x): return x.split(' ')

    def joiner(x): return ' '.join(x)


input_file = args.input_file

print('will import file {0}'.format(input_file))

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
    if utterance['tags'] is not None:
        for t in utterance['tags']:
            if t in tags_counter.keys():
                tags_counter[t] += 1
            else:
                tags_counter[t] = 1
    tokens = splitter(utterance['text'])
    total_length_tokens += len(tokens)
    total_length_chars += len(utterance['text'])
    if utterances_count % 1000 == 0:
        elapsed = time.time() - start_time
        print('so far, processed {0} utterances containing {1} tokens, it took {2} seconds [{3} utterances per second]'
              .format(utterances_count, total_length_tokens, elapsed, utterances_count / elapsed))
    if len(utterance['text']) > max_tokens:
        max_tokens = len(utterance['text'])
        if len(utterance['text']) > 320:
            longest_description = utterance['text'][:160] + ' ...[truncated]... ' + utterance['text'][-160:]
reader.close()

elapsed = time.time() - start_time
print('processed {0} utterances containing {1} tokens, it took {2} seconds [{3} utterances per second]'
      .format(utterances_count, total_length_tokens, elapsed, utterances_count/elapsed))
print('average tokens per sentence: {0}'.format(total_length_tokens/utterances_count))
print('average characters per sentence: {0}'.format(total_length_chars/utterances_count))
print('the longest utterance had {0} characters, it was:\n{1}\n'.format(max_tokens, longest_description))
print('found {0} types of tags'.format(len(tags_counter.keys())))

max_rank = 30
print('{0} most common tags'.format(max_rank))
sorted_tags = sorted(tags_counter.items(), key=operator.itemgetter(1), reverse=True)[:max_rank]
for k, v in sorted_tags:
    print('{0}: {1}'.format(k, v))

