import argparse
from datetime import datetime
import json

import requests
from bs4 import BeautifulSoup


parser = argparse.ArgumentParser(
    description="download comments to articles from HackerNews")
parser.add_argument(
    "-output_file",
    type=str,
    help="the one-JSON-per-line serialized file",
    default="hackernews_utterances.json")

args = parser.parse_args()

response = requests.request("GET", "https://news.ycombinator.com/")
soup = BeautifulSoup(response.text, 'html.parser')

article_comment_links = set()

for link in soup.find_all('a'):
    if 'item?id=' in link.get('href'):
        link.get('href')
        article_comment_links.add(
            "https://news.ycombinator.com/" +
            link.get('href'))

print(
    'found {0} articles from the HackerNews homepage'.format(
        len(article_comment_links)))

comment_count = 0
for article_link in article_comment_links:
    print('examining ' + article_link)
    response = requests.request("GET", article_link)
    soup = BeautifulSoup(response.text, 'html.parser')
    with open(args.output_file, "a+") as writer:
        for comment_box in soup.find_all("td", class_="default"):
            print(comment_box.contents)
            comment = comment_box.contents[2]
            clean_comment = comment.get_text().replace(
                '\n', ' ').rsplit(
                'reply', maxsplit=1)[0].strip()
            username = comment_box.contents[0].contents[0].contents[1].get_text()
            # relative time can later be processed with https://github.com/comtravo/ctparse
            relative_time = comment_box.contents[0].contents[0].contents[3].get_text()

            writer.write(json.dumps({"text": clean_comment,
                                     "scrape_timestamp": datetime.now().isoformat(),
                                     "relative_time": relative_time,
                                     "source": article_link,
                                     "username": username,
                                     "tags": []}))
            writer.write('\n')
            print('-----')
            comment_count += 1


writer.close()
print(
    'Done! Processed {0} comments from {1} article links'.format(
        comment_count,
        len(article_link)))
