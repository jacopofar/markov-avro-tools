This is a collection of Python 3 tools to ingest text from various sources (RSS feeds, text files, etc.), build frequency counts and build Markov chains to generate text.

Why?
====
Because Markov chains and frequency tables are interesting, but in spite of being fairly simple it's hard to find a flexible tool to build them. 

Moreover, the intermediate tagged avro files are suitable for other NPL tasks.

Usage
=====
Install requisites using `pip3 install -r requirements.txt` in your virtualenv.
Then, use one of the scripts to load data from a source (text files, Twitter, RSS feeds, mail dumps) and produce an avro utterance file. This file contains the texts from the source and, when possible, metadata such as the timestamp and tags (for example, in case of a mailbox each mail is tagged with the from and to addresses, for Twitter the hashtags, language and location are made into tags).
 
 Now that avro files are ready, you need Redis. If you have Docker, you can use `docker run -p 6379:6379 redis`.
  
 Then use `markov_from_avro.py` to build a model and generate text based on it. Use `-h`  on this or any script to have a description of its parameters. You can store multiple models on the same Redis instance differentiating with the _prefix_ parameter.
 
 
 __N-gram size__ you can specify the length of N-grams with the keylen parameter. Note that you have to use the same keylength to build and to generate the model.
 
 __tokenizer__ you can split the text by words, NLTK tokens or by characters, see the _tokenize_ parameter
 
 __mix models__ the build function doesn't empty Redis when starting, so you can invoke it on multiple avro files and mix the models. The resulting chain will mix N-grams from the various given sources. You can use tags to build models only for a specific subset of utterances (e.g. hashtags on Twitter or senders for mailboxes)

Additionally, you can build token frequency tables and get a few statistics
 
Supported Sources
=======
* RSS feeds `avro_from_feed.py`
* Twitter `avro_from_twitter.py`
* Plain text files in a folder `avro_from_folder.py` 
* mail dumps (Gmail by using Google Takeout, other mail clients) `avro_from_mailbox.py`
* HackerNews comments `avro_from_hackernews.py`
* Chat logs from Adium (IRC, Telegram), see [this other repo](https://github.com/jacopofar/adium-to-avro)
* Wikiquote dump `avro_from_wikiquote.py`
* Wikipedia dumps `plaintext_from_wikidump.py`

Example usage
=======
Optional but suggested: create and activate the virtualenv with `python3 -m venv venv && source venv/bin/activate`

Install libraries with `pip3 install -r requirements.txt`

Let's try with HackerNews: `python3 avro_from_hackernews.py`. This will create the file `hackernews_utterances.avro`, that you can inspect with [rq](https://github.com/dflemstr/rq)

Now start a Redis instance:

`docker run -p 6379:6379 redis`

and finally build the markov chain with
`python3 markov_from_avro.py build -input_file hackernews_utterances.avro -keylen 2 -tokenizer split`

this will use 2-grams based on Python `split` function.

Generate text based on it with `python3 markov_from_avro.py generate -number 1 -keylen 2` (be careful of using the same keylen parameter).

I got:


> Perhaps someone in the group. With slack, there's on boarding and a link between high cholesterol and cardiovascular disease has been compiling everything from node into browser - emulating a lot of expressive power, but hey, no halting problem. The obvious thing to forget on HN, flat earth? Chemtrails? After a recent article in the mortgage & finance markets.

Now let's try building one from characters:

`python3 markov_from_avro.py build -input_file hackernews_utterances.avro -keylen 4 -tokenizer char -markov_prefix hn_c`

this time the keylen is different and with the prefix _nh_c_ there's no confusion with the previous ones.

with `python3 markov_from_avro.py generate -number 1 -keylen 4 -markov_prefix hn_c -tokenizer char`

I got:

> Just now the phrased amphetaming with is not benefit is a differ own the me, ever you can in other the bust is going somethods like south Arduino Hearing: http://www.bbc.co.uk/newsletting than employ peace that clearnings - tree wife industry prove to benefit, but does to have see wholested.In that is plane featurate any of then that my photos (to states looks. Does to tellennials are only would had side overdoing a versatistick all TSX (HSW136) is for act the UK.

Works with Python 3.6, enjoy!