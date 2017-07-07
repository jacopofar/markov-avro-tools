import requests
from bs4 import BeautifulSoup
import feedparser
import re
from html.parser import HTMLParser
import time
from datetime import datetime

import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

import argparse


class MLStripper(HTMLParser):
    def error(self, message):
        print('error in HTML parser:' + message)

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


# original from https://stackoverflow.com/questions/7883581/automatically-extracting-feed-links-atom-rss-etc-from-webpages
def detect_feed_in_HTML(raw_content):
    """ examines a raw HTML content for referenced feeds.

    This is achieved by detecting all ``link`` tags that reference a feed in HTML.
    Then, return the shorter. This is do avoid comment feeds

    :param raw_content: the complete page HTML
    :return: the most likely RSS feed, if any
    :rtype: str
    """
    html = BeautifulSoup(raw_content, 'html.parser')
    # find all links that have an "alternate" attribute
    feed_urls = html.findAll("link", rel="alternate", type="application/rss+xml")
    feed_urls = feed_urls + html.findAll("link", rel="alternate", type="application/atom+xml")

    # extract URL
    result = []
    for feed_link in feed_urls:
        url = feed_link.get("href", None)
        # if a valid URL is there
        if url:
            result.append(url)
    return min(result, key=len)


def strip_formatting_tags(raw_html):
    """ removes simple formatting tags from the html as text, using regexes,
    preparing it fro furthe prrocessing with proper methos
    :param raw_html:
    :return: the raw HTML (as a string) without a few tags
    """
    return raw_html.replace('</strong>', '')\
        .replace('<strong>', '') \
        .replace('<b>', '') \
        .replace('</b>', '')


def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element)):
        return False
    return True


def to_iso_date (original_date):
    p = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.?\d*(Z|\+\d{2}:\d{2})")
    if p.match(original_date):
        return original_date
    return datetime.strptime(original_date, '%a, %d %b %Y %H:%M:%S %z').strftime('%Y-%m-%d %H:%M:%S.%f')


parser = argparse.ArgumentParser(description="download articles from a feed")
parser.add_argument("url", type=str, help="the URL of a page containing the RSS rel element")
parser.add_argument("-output_file", type=str, help="the avro serialized file", default="utterances.avro")

feature_parser = parser.add_mutually_exclusive_group(required=False)
feature_parser.add_argument('--detect-rss', dest='direct_feed', action='store_true', help="wether the given URL is a site page (default) from which to extract the feed address, or directly the RSS feed URL")
feature_parser.add_argument('--no-detect-rss', dest='direct_feed', action='store_false', help="wether the given URL is a site page (default) from which to extract the feed address, or directly the RSS feed URL")
parser.set_defaults(direct_feed=True)


args = parser.parse_args()

schema = avro.schema.Parse(open("corpus_utterance.avsc", "rb").read().decode('utf-8'))
writer = DataFileWriter(open(args.output_file, "wb"), DatumWriter(), schema)
url = args.url

print('will try to retrieve feed URL and related articles from {0} and save them in {1}'.format(url, args.output_file))

start_time = time.time()
utterances_count = 0
articles_count = 0
headers = {}
response = requests.request("GET", url, headers=headers)
if args.direct_feed:
	feed_url = url
else:
	feed_url = detect_feed_in_HTML(response.text)
print('feed URL: ' + feed_url)

d = feedparser.parse(feed_url)
print('feed title: ' + d['feed']['title'])

for ent in d.entries:
    time.sleep(3)
    # convert from something like 'Mon, 10 Oct 2016 12:21:36 +0000' to ISO timestamps, if not already ISO
    decent_format_date = to_iso_date(ent.published)
    print('processing article ' + ent.link)
    articles_count += 1
    headers = {}
    response = requests.request("GET", ent.link, headers=headers)
    soup = BeautifulSoup(strip_formatting_tags(response.text), 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = list(filter(lambda x: len(x) > 160, filter(visible, texts)))
    print('there are ' + str(len(visible_texts)) + ' lines in this article')
    for l in visible_texts:
        utterances_count += 1
        writer.append({"text": l, "timestamp": decent_format_date, "source": ent.link, "tags": [url]})

elapsed = time.time() - start_time
writer.close()

print('processed {0} utterances from {1} articles, it took {2} seconds [{3} articles per second]'
      .format(utterances_count, articles_count, elapsed, articles_count/elapsed))
