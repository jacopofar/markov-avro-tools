import requests
from bs4 import BeautifulSoup

import avro
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

import argparse

parser = argparse.ArgumentParser(description="download articles from HackerNews")
parser.add_argument("-output_file", type=str, help="the avro serialized file", default="hackernews_utterances.avro")

args = parser.parse_args()

schema = avro.schema.Parse(open("corpus_utterance.avsc", "rb").read().decode('utf-8'))
writer = DataFileWriter(open(args.output_file, "wb"), DatumWriter(), schema)

response = requests.request("GET", "https://news.ycombinator.com/")
soup = BeautifulSoup(response.text, 'html.parser')

article_comment_links = set()

for link in soup.find_all('a'):
    if 'item?id=' in link.get('href'):
        link.get('href')
        article_comment_links.add("https://news.ycombinator.com/" + link.get('href'))

print('found {0} articles from the HackerNews homepage'.format(len(article_comment_links)))

comment_count = 0
for article_link in article_comment_links:
    print('examining ' + article_link)
    response = requests.request("GET", article_link)
    soup = BeautifulSoup(response.text, 'html.parser')
    for comment in soup.find_all("div", class_="comment"):
        clean_comment = comment.get_text().replace('\n', ' ').rsplit('reply', maxsplit=1)[0].strip()
        print(clean_comment)
        writer.append({"text": clean_comment, "timestamp": "", "source": article_link, "tags": []})
        print('-----')
        comment_count += 1


writer.close()
print('Done! Processed {0} comments from {1} article links'.format(comment_count, len(article_link)))