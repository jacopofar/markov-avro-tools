import bz2
import xml.sax
import mwparserfromhell
import argparse


parser = argparse.ArgumentParser(description="extract plaintext from a Wikiquote compressed dump")
parser.add_argument("-dump_path", type=str, help="the path of the compressed Wikipedia dump, compressed")
parser.add_argument("-output_file", type=str, help="the plain text file", default="plaintext_wiki.txt")

args = parser.parse_args()
bz_file = bz2.BZ2File(args.dump_path)

plainfile = open(args.output_file, "w")


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
            # blacklist to exclude most of the non-article pages. Lazy, but works
            if self.current_title.split(':')[0] in ['Categoria', 'Wikipedia', 'File', 'Progetto', ' Template', 'Portale', 'Discussione', 'Category', 'Project', 'Portal', 'Talk']:
                print(f'\t\tskipping non-article {self.current_title}')
                self.current_tag = None
                return
            self.page_count += 1
            print(f'Article {self.page_count}: {self.current_title} - {len(self.current_raw_page)} characters')
            wikicode = mwparserfromhell.parse(self.current_raw_page)
            plaintext = wikicode.strip_code()
            useful_text = ''
            for line in plaintext.split('\n'):
                if len(line) > 100:
                    useful_text += line.strip() + '\n'
            plainfile.write(useful_text)
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
plainfile.close()
