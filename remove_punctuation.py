import argparse
import re

parser = argparse.ArgumentParser(description="creates a lowercase, no-punctuation copy of a plain text file")
parser.add_argument("-input_file", type=str, help="the path of the input file")
parser.add_argument("-output_file", type=str, help="the output text file", default="nopunct.txt")

args = parser.parse_args()

plainfile = open(args.output_file, "w")

for line in open(args.input_file, encoding='utf8'):
    # note that it uses Unicode letters, so accented characters or similar are took into account. No idea about CJK
    processed = ' '.join(re.sub(r'\W', ' ', line.lower()).split())
    plainfile.write(processed + '\n')

plainfile.close()
