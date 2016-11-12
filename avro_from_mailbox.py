import time
import mailbox
import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from html.parser import HTMLParser
from datetime import datetime

import argparse


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return ''.join(self.fed)


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def date_to_iso(raw_date):
    # remove double spaces. Why are dates so dirty ?
    raw_date = raw_date.replace('  ', ' ')
    # from a format like 'Sun, 24 Aug 2014 07:07:57 -0700 (PDT)' remove the last part
    raw_date = ' '.join(raw_date.split(' ')[:6])
    return datetime.strptime(raw_date, '%a, %d %b %Y %H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S.%f')


def iterate_bodies(mbox):
    for message in mbox:
        date = date_to_iso(message.get('Date'))
        if message.is_multipart():
            for part in message.walk():
                if part.get_content_type() == 'text/plain':
                    t = strip_tags(part.get_payload())
                    # simple heuristic to ignore base64 data and empty mails. Works for european languages
                    if len(t) > 10:
                        if t.count(' ') / len(t) > 0.05:
                            yield {'text': part.get_payload(),
                                   'date': date,
                                   "from": message.get('From'),
                                   "to": message.get('To'),
                                   "reply_to": message.get('Reply-to'),
                                   "subject": message.get('Subject')}
        else:
            t = strip_tags(message.get_payload())
            # simple heuristic to ignore base64 data. Works for european languages
            if len(t) > 10:
                if t.count(' ') / len(t) > 0.05:
                    yield {"text": message.get_payload(),
                           "date": date,
                           "from": message.get('From'),
                           "to": message.get('To'),
                           "reply_to": message.get('Reply-to'),
                           "subject": message.get('Subject')}


parser = argparse.ArgumentParser(
    description="store utterances from a mbox file, like the one from gmal (google takeout)")
parser.add_argument("i", type=str, help="the input file")
parser.add_argument("-output_file", type=str, help="the avro serialized file", default="mail_utterances.avro")

args = parser.parse_args()

schema = avro.schema.Parse(open("corpus_utterance.avsc", "rb").read().decode('utf-8'))
writer = DataFileWriter(open(args.output_file, "wb"), DatumWriter(), schema)
mbox = mailbox.mbox(args.i)
start_time = time.time()
tot = 0
utterance_count = 0

for m in iterate_bodies(mbox):
    tot += 1
    utterance_count += 1
    print('processing mail number {0} {1} {2}'.format(tot, m['subject'], m['date']))
    utterance_count += 1
    writer.append({"text": m['text'],
                   "source": '{0} {1}:{2}'.format(m['from'], m['to'], m['subject']),
                   "tags": ['from:' + str(m['from']), 'to:' + str(m['to']), 'rt:' + str(m['reply_to'])],
                   "timestamp": m['date']
                   })

elapsed = time.time() - start_time
writer.close()

print('processed {0} utterances from {1} mails, it took {2} seconds [{3} files per second]'
      .format(utterance_count, tot, elapsed, tot / elapsed))
