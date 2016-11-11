
import time
from os import listdir, path
import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

import argparse


parser = argparse.ArgumentParser(description="store utterances from a folder containing plain text files")
parser.add_argument("-folder", type=str, help="the folder path", default='.')
parser.add_argument("-output_file", type=str, help="the avro serialized file", default="utterances.avro")
parser.add_argument("-extension", type=str, help="if specified, load only files with this extension")

args = parser.parse_args()

schema = avro.schema.Parse(open("corpus_utterance.avsc", "rb").read().decode('utf-8'))
writer = DataFileWriter(open(args.output_file, "wb"), DatumWriter(), schema)

start_time = time.time()
tot = 0
utterance_count = 0
extension = args.extension

for fname in listdir(args.folder):
    if extension is not None:
        if str.endswith(fname, extension):
            continue
    tot += 1
    print('processing file number {0} {1}'.format(tot, fname))
    linenum = 0
    for line in open(path.join(args.folder, fname), encoding='utf-8'):
        linenum += 1
        if len(line) < 10:
            continue
        utterance_count += 1
        writer.append({"text": line, "source": '{0}#{1}'.format(fname, linenum), "tags": [fname]})


elapsed = time.time() - start_time
writer.close()

print('processed {0} utterances from {1} files, it took {2} seconds [{3} files per second]'
      .format(utterance_count, tot, elapsed, tot/elapsed))
