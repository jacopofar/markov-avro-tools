import bz2
import xml.sax
import mwparserfromhell
import argparse

import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


parser = argparse.ArgumentParser(description="extract quotes from a Wikiquote dump")
parser.add_argument("dump_path", type=str, help="the path of the Wikiquote dump, compressed")
parser.add_argument("-output_file", type=str, help="the avro serialized file", default="utterances.avro")

args = parser.parse_args()
bz_file = bz2.BZ2File(args.dump_path)

schema = avro.schema.Parse(open("corpus_utterance.avsc", "rb").read().decode('utf-8'))
writer = DataFileWriter(open(args.output_file, "wb"), DatumWriter(), schema)


class WikiDumpHandler(xml.sax.ContentHandler):
    def __init__(self):
        super().__init__()
        self.page_count = 0
        self.current_quotes = 0
        self.current_title = ''
        self.current_raw_page = ''
        self.current_tag = None

    def characters(self, content):
        if self.current_tag == 'title':
            self.current_title += content
        if self.current_tag == 'text':
            self.current_raw_page += content

    def endElement(self, name):
        if name == 'title':
            self.current_tag = None

        if name == 'text':
            # TODO use mediawiki namespaces to identify special pages. This is only for EN and IT
            if self.current_title.split(':') in ['Category', 'Categoria', 'Wikiquote']:
                return
            self.page_count += 1
            print('Article {3}: {0} - {1} characters in {2} lines, {4} quotes so far'.format(self.current_title, len(self.current_raw_page), len(self.current_raw_page.split('\n')), self.page_count, self.current_quotes))
            wikicode = mwparserfromhell.parse(self.current_raw_page)
            # a quote is marked by a Tag with a * as the content, then the content is inside a list of Text nodes
            in_quote = False
            current_quote = ''
            for n in wikicode.nodes:
                if in_quote:
                    if type(n) in [mwparserfromhell.nodes.Text, mwparserfromhell.nodes.Wikilink]:
                        current_quote += mwparserfromhell.parse(str(n)).strip_code().replace('\n', ' ')
                    if type(n) not in [mwparserfromhell.nodes.Text, mwparserfromhell.nodes.Wikilink] or len(n.replace('\n', ' ').strip()) == 0:
                        if len(current_quote) > 10:
                            writer.append(
                                {"text": current_quote, "source": self.current_title, "tags": []})
                            self.current_quotes += 1

                        current_quote = ''
                        if type(n) is mwparserfromhell.nodes.Tag and n == '*':
                            in_quote = True
                else:
                    if type(n) is mwparserfromhell.nodes.Tag and n == '*':
                        in_quote = True
            self.current_tag = None

    def startElement(self, name, attrs):
        if name == 'title':
            self.current_tag = 'title'
            self.current_title = ''

        if name == 'text':
            self.current_tag = 'text'
            self.current_raw_page = ''


parser = xml.sax.make_parser()
parser.setContentHandler(WikiDumpHandler())
parser.parse(bz_file)
writer.close()
